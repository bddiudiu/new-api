package controller

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuotaLogsDatabaseMatrix(t *testing.T) {
	for _, kind := range []string{"sqlite", "mysql", "postgres", "clickhouse"} {
		t.Run(kind, func(t *testing.T) {
			dsn := os.Getenv("TEST_" + strings.ToUpper(kind) + "_DSN")
			if kind != "sqlite" && dsn == "" {
				t.Skip("TEST_" + strings.ToUpper(kind) + "_DSN is not configured")
			}
			require.NoError(t, i18n.Init())
			previousDB, previousLogDB := model.DB, model.LOG_DB
			previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
			previousRedis, previousMaster, previousConsume, previousPath := common.RedisEnabled, common.IsMasterNode, common.LogConsumeEnabled, common.SQLitePath
			t.Cleanup(func() {
				model.DB, model.LOG_DB = previousDB, previousLogDB
				common.SetDatabaseTypes(previousMain, previousLog)
				common.RedisEnabled, common.IsMasterNode, common.LogConsumeEnabled, common.SQLitePath = previousRedis, previousMaster, previousConsume, previousPath
			})
			mainKind, mainDSN := kind, dsn
			if kind == "clickhouse" {
				mainKind, mainDSN = "sqlite", ""
			}
			mainDB, _ := newAuditTestDatabase(t, mainKind, mainDSN)
			_, logDSN := newAuditTestDatabase(t, kind, dsn)
			model.DB = mainDB
			common.SetMainDatabaseType(common.DatabaseType(mainKind))
			common.RedisEnabled, common.IsMasterNode, common.LogConsumeEnabled = false, true, true
			require.NoError(t, mainDB.AutoMigrate(&model.User{}, &model.Checkin{}, &model.Log{}))
			if kind == "sqlite" {
				common.SQLitePath, logDSN = logDSN, "local"
			}
			t.Setenv("LOG_SQL_DSN", logDSN)
			require.NoError(t, model.InitLogDB())
			connection, err := model.LOG_DB.DB()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, connection.Close()) })
			versionSQL := "SELECT version()"
			if kind == "sqlite" {
				versionSQL = "SELECT sqlite_version()"
			}
			var version string
			require.NoError(t, model.LOG_DB.Raw(versionSQL).Scan(&version).Error)
			t.Logf("main database: %s; independent log database: %s %s", mainKind, kind, version)

			adminPAT, userPAT := "quota-admin-test-credential", "quota-user-test-credential"
			admin := model.User{Username: "quota-admin", Role: common.RoleAdminUser, Status: common.UserStatusEnabled, AuthVersion: 1, AccessToken: &adminPAT, AffCode: "quota-admin"}
			user := model.User{Username: "quota-owner", Group: "business", Role: common.RoleCommonUser, Status: common.UserStatusEnabled, AuthVersion: 1, AccessToken: &userPAT, AffCode: "quota-owner", Quota: 1000}
			require.NoError(t, mainDB.Create(&admin).Error)
			require.NoError(t, mainDB.Create(&user).Error)
			// Preserve an existing business row across repeated startup. Its token
			// counts also prove that all quota-consuming types enter statistics.
			require.NoError(t, model.LOG_DB.Create(&model.Log{UserId: user.Id, Username: user.Username, Group: user.Group, Type: 8, Quota: 100, PromptTokens: 7, CompletionTokens: 3, CreatedAt: time.Now().Unix(), RequestId: "retained-meeting"}).Error)
			require.NoError(t, model.InitLogDB())
			secondConnection, err := model.LOG_DB.DB()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, secondConnection.Close()) })
			var retained model.Log
			require.NoError(t, model.LOG_DB.Where("request_id = ?", "retained-meeting").Take(&retained).Error)
			assert.Equal(t, 100, retained.Quota)

			router := gin.New()
			router.Use(middleware.RequestId())
			router.POST("/api/log/quota", middleware.AdminAuth(), RecordLogWithQuota)
			router.POST("/api/user/self/checkin", middleware.UserAuth(), DoCheckin)
			router.GET("/api/log/stat", middleware.AdminAuth(), GetLogsStat)
			for i := range 13 {
				logType := i + 1
				other := `{"public":"visible","large_id":9007199254740993,"admin_info":{"operator":"admin-only"},"root_info":{"diagnostic":"root-only"},"audit_info":{"trace":"audit-only"},"channel_id":99,"reject_reason":"legacy-reason"}`
				response := performQuotaLogRequest(t, router, http.MethodPost, "/api/log/quota", map[string]any{"user_id": user.Id, "log_type": logType, "quota": 10, "content": "  业务额度  ", "other": other}, adminPAT)
				require.Contains(t, response.Body.String(), `"success":true`)
				var log model.Log
				require.NoError(t, model.LOG_DB.Where("request_id = ?", response.Header().Get(common.RequestIdKey)).Take(&log).Error)
				assert.Equal(t, logType, log.Type)
				assert.Equal(t, 10, log.Quota)
				assert.Equal(t, "业务额度", log.Content)
				assert.Equal(t, user.Group, log.Group)
				assert.Equal(t, user.Username, log.Username)
				assert.Contains(t, log.Other, "9007199254740993")
				assert.Contains(t, log.Other, "root-only")
				model.FormatAdminLogs([]*model.Log{&log})
				assert.Contains(t, log.Other, "admin-only")
				assert.NotContains(t, log.Other, "root-only")
			}
			logs, total, err := model.GetUserLogs(user.Id, 13, 0, 0, "", "", 0, 20, user.Group, "", "")
			require.NoError(t, err)
			assert.EqualValues(t, 1, total)
			require.Len(t, logs, 1)
			assert.JSONEq(t, `{"public":"visible","large_id":9007199254740993}`, logs[0].Other)
			stat := performQuotaLogRequest(t, router, http.MethodGet, "/api/log/stat?username=quota-owner&group=business", nil, adminPAT)
			var stats struct {
				Success bool       `json:"success"`
				Data    model.Stat `json:"data"`
			}
			require.NoError(t, common.Unmarshal(stat.Body.Bytes(), &stats))
			require.True(t, stats.Success)
			assert.Equal(t, 140, stats.Data.Quota)
			assert.Equal(t, 5, stats.Data.Rpm)
			assert.Equal(t, 10, stats.Data.Tpm)
			assert.Equal(t, 10, model.SumUsedToken(0, 0, 0, "", user.Username, ""))
			require.NoError(t, mainDB.First(&user, user.Id).Error)
			assert.Equal(t, 1000, user.Quota, "writing business logs must not move the wallet")
			var mainLogCount, operationAudits int64
			require.NoError(t, mainDB.Model(&model.Log{}).Count(&mainLogCount).Error)
			assert.Zero(t, mainLogCount)
			require.NoError(t, model.LOG_DB.Model(&model.AuditLog{}).Where("category = ?", model.AuditCategoryOperation).Count(&operationAudits).Error)
			assert.EqualValues(t, 13, operationAudits)

			for _, tc := range []struct {
				name    string
				changes map[string]any
			}{
				{"missing user", map[string]any{"user_id": 0}},
				{"unknown user", map[string]any{"user_id": 999999}},
				{"unknown type", map[string]any{"log_type": 0}},
				{"unsupported type", map[string]any{"log_type": 14}},
				{"empty content", map[string]any{"content": " \n "}},
				{"negative consumption", map[string]any{"quota": -1}},
				{"oversized quota", map[string]any{"quota": common.MaxWalletQuota + 1}},
				{"malformed metadata", map[string]any{"other": "{"}},
				{"array metadata", map[string]any{"other": "[]"}},
				{"null metadata", map[string]any{"other": "null"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					body := map[string]any{"user_id": user.Id, "log_type": 13, "quota": 10, "content": "invalid"}
					for key, value := range tc.changes {
						body[key] = value
					}
					response := performQuotaLogRequest(t, router, http.MethodPost, "/api/log/quota", body, adminPAT)
					assert.Contains(t, response.Body.String(), `"success":false`)
				})
			}
			for _, credential := range []string{"", userPAT} {
				response := performQuotaLogRequest(t, router, http.MethodPost, "/api/log/quota", map[string]any{"user_id": user.Id, "log_type": 13, "quota": 10, "content": "unauthorized"}, credential)
				assert.Contains(t, []int{http.StatusUnauthorized, http.StatusForbidden}, response.Code)
			}
			common.LogConsumeEnabled = false
			response := performQuotaLogRequest(t, router, http.MethodPost, "/api/log/quota", map[string]any{"user_id": user.Id, "log_type": 2, "quota": 10, "content": "disabled"}, adminPAT)
			assert.Contains(t, response.Body.String(), `"success":false`)
			var logCount int64
			require.NoError(t, model.LOG_DB.Model(&model.Log{}).Count(&logCount).Error)
			assert.EqualValues(t, 14, logCount, "rejected writes must leave the log store unchanged")
			response = performQuotaLogRequest(t, router, http.MethodPost, "/api/log/quota", map[string]any{"user_id": user.Id, "log_type": 1, "quota": -10, "content": "signed adjustment"}, adminPAT)
			require.Contains(t, response.Body.String(), `"success":true`)
			var adjustment model.Log
			require.NoError(t, model.LOG_DB.Where("request_id = ?", response.Header().Get(common.RequestIdKey)).Take(&adjustment).Error)
			assert.Equal(t, -10, adjustment.Quota)

			checkinSetting := operation_setting.GetCheckinSetting()
			previousCheckin := *checkinSetting
			t.Cleanup(func() { *checkinSetting = previousCheckin })
			checkinSetting.Enabled, checkinSetting.MinQuota, checkinSetting.MaxQuota = true, 17, 17
			response = performQuotaLogRequest(t, router, http.MethodPost, "/api/user/self/checkin", nil, userPAT)
			require.Contains(t, response.Body.String(), `"success":true`)
			var checkinLog model.Log
			require.NoError(t, model.LOG_DB.Where("request_id = ?", response.Header().Get(common.RequestIdKey)).Take(&checkinLog).Error)
			assert.Equal(t, 11, checkinLog.Type)
			assert.Equal(t, 17, checkinLog.Quota)
			require.NoError(t, mainDB.First(&user, user.Id).Error)
			assert.Equal(t, 1017, user.Quota, "check-in grants its reward exactly once")
		})
	}
}

func performQuotaLogRequest(t *testing.T, router *gin.Engine, method, path string, body any, credential string) *httptest.ResponseRecorder {
	t.Helper()
	data, err := common.Marshal(body)
	require.NoError(t, err)
	request := httptest.NewRequest(method, path, strings.NewReader(string(data)))
	request.Header.Set("Content-Type", "application/json")
	if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	request.Header.Set("User-Agent", fmt.Sprintf("quota-log-%s-test", method))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}
