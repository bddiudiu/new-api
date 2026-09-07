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
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateUserDefaultTokenIsAtomic(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		enabled, tokenTable, success bool
	}{
		{"enabled", true, true, true},
		{"disabled", false, true, true},
		{"token_insert_failure", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			admin, _ := setupAccessTokenAudit(t)
			previous := constant.GenerateDefaultToken
			constant.GenerateDefaultToken = tc.enabled
			t.Cleanup(func() { constant.GenerateDefaultToken = previous })
			if tc.tokenTable {
				require.NoError(t, model.DB.AutoMigrate(&model.Token{}))
			}
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Set("id", admin.Id)
			ctx.Set("role", common.RoleAdminUser)
			ctx.Set("username", admin.Username)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/api/user/", strings.NewReader(`{"username":"coocare-new-user","password":"Long-test-password-2026!","group":"default","role":1}`))
			CreateUser(ctx)
			var response struct {
				Success bool `json:"success"`
				UserID  int  `json:"userId"`
			}
			require.NoError(t, common.Unmarshal(recorder.Body.Bytes(), &response))
			require.Equal(t, tc.success, response.Success, recorder.Body.String())
			var users []model.User
			require.NoError(t, model.DB.Where("username = ?", "coocare-new-user").Find(&users).Error)
			if !tc.success {
				assert.Empty(t, users, "a token failure must roll back user creation")
				return
			}
			require.Len(t, users, 1)
			assert.Equal(t, users[0].Id, response.UserID)
			assert.Equal(t, "default", users[0].Group)
			assert.NotEqual(t, "Long-test-password-2026!", users[0].Password)
			var tokens []model.Token
			require.NoError(t, model.DB.Where("user_id = ?", response.UserID).Find(&tokens).Error)
			if tc.enabled {
				require.Len(t, tokens, 1)
				assert.NotEmpty(t, tokens[0].Key)
				assert.True(t, tokens[0].UnlimitedQuota)
				assert.NotContains(t, recorder.Body.String(), tokens[0].Key)
			} else {
				assert.Empty(t, tokens)
			}
			var audits []model.AuditLog
			require.NoError(t, model.LOG_DB.Where("user_id = ?", admin.Id).Find(&audits).Error)
			require.NotEmpty(t, audits)
			encoded, err := common.Marshal(audits)
			require.NoError(t, err)
			assert.NotContains(t, string(encoded), "Long-test-password-2026!")
			if len(tokens) > 0 {
				assert.NotContains(t, string(encoded), tokens[0].Key)
			}
		})
	}
}

func TestQuotaExpiryDatabaseMatrix(t *testing.T) {
	previousDB, previousLogDB := model.DB, model.LOG_DB
	previousMain, previousLog := common.MainDatabaseType(), common.LogDatabaseType()
	previousRedis, previousMaster, previousSQLite := common.RedisEnabled, common.IsMasterNode, common.SQLitePath
	previousRules := operation_setting.GetQuotaExpirySetting().Rules
	common.RedisEnabled, common.IsMasterNode = false, true
	operation_setting.GetQuotaExpirySetting().Rules = []operation_setting.LogTypeExpiryRule{{Label: "Activity", LogType: model.LogTypeActive, ExpireDays: 2}}
	t.Cleanup(func() {
		model.DB, model.LOG_DB = previousDB, previousLogDB
		common.SetDatabaseTypes(previousMain, previousLog)
		common.RedisEnabled, common.IsMasterNode, common.SQLitePath = previousRedis, previousMaster, previousSQLite
		operation_setting.GetQuotaExpirySetting().Rules = previousRules
	})
	for _, engine := range []struct {
		name, env string
		typ       common.DatabaseType
	}{
		{"sqlite", "", common.DatabaseTypeSQLite},
		{"mysql", "AUDIT_MYSQL_DSN", common.DatabaseTypeMySQL},
		{"postgres", "AUDIT_POSTGRES_DSN", common.DatabaseTypePostgreSQL},
	} {
		t.Run(engine.name, func(t *testing.T) {
			dsn := os.Getenv(engine.env)
			if engine.env != "" && dsn == "" {
				t.Skip(engine.env + " is not configured")
			}
			for _, upgrade := range []bool{false, true} {
				for _, split := range []bool{false, true} {
					t.Run(fmt.Sprintf("upgrade=%v/split_logs=%v", upgrade, split), func(t *testing.T) {
						db, isolatedDSN := newAuditTestDatabase(t, engine.name, dsn)
						logDB := db
						logDSN := ""
						if split {
							logDB, logDSN = newAuditTestDatabase(t, engine.name, dsn)
						}
						if split && engine.name == "sqlite" {
							t.Setenv("LOG_SQL_DSN", "local")
						} else {
							t.Setenv("LOG_SQL_DSN", logDSN)
						}
						if engine.name == "sqlite" {
							common.SQLitePath = isolatedDSN
							t.Setenv("SQL_DSN", "local")
						} else {
							t.Setenv("SQL_DSN", isolatedDSN)
						}
						model.DB, model.LOG_DB = db, logDB
						common.SetDatabaseTypes(engine.typ, engine.typ)
						versionSQL := "SELECT version()"
						if engine.name == "sqlite" {
							versionSQL = "SELECT sqlite_version()"
						}
						var version string
						require.NoError(t, db.Raw(versionSQL).Scan(&version).Error)
						t.Logf("database version: %s", version)
						createdAt := time.Now().AddDate(0, 0, -5).Unix()
						if upgrade {
							require.NoError(t, db.AutoMigrate(&releasedAuditUser{}))
							require.NoError(t, logDB.AutoMigrate(&releasedAuditLog{}))
							require.NoError(t, db.Create(&releasedAuditUser{Id: 1, Username: "legacy-quota-owner", Password: "placeholder", AffCode: "quota-owner", Quota: 60}).Error)
							require.NoError(t, logDB.Create(&releasedAuditLog{UserId: 1, Type: model.LogTypeActive, Quota: 100, CreatedAt: createdAt, Content: "legacy activity"}).Error)
							require.NoError(t, logDB.Create(&releasedAuditLog{UserId: 1, Type: model.LogTypeVoice, Quota: 40, CreatedAt: createdAt + 3600, Content: "legacy voice"}).Error)
						}
						for i := 0; i < 2; i++ {
							common.SQLitePath = isolatedDSN
							require.NoError(t, model.InitDB())
							if split && engine.name == "sqlite" {
								common.SQLitePath = logDSN
							}
							require.NoError(t, model.InitLogDB())
							common.SQLitePath = isolatedDSN
						}
						require.True(t, model.LOG_DB.Migrator().HasIndex(&model.Log{}, "idx_logs_type_created_id"))
						require.True(t, model.LOG_DB.Migrator().HasIndex(&model.Log{}, "idx_user_id_id"))
						if !upgrade {
							require.NoError(t, model.DB.Create(&model.User{Id: 1, Username: "fresh-quota-owner", Password: "placeholder", AffCode: "quota-owner", Quota: 60}).Error)
							require.NoError(t, model.LOG_DB.Create(&model.Log{UserId: 1, Type: model.LogTypeActive, Quota: 100, CreatedAt: createdAt}).Error)
							require.NoError(t, model.LOG_DB.Create(&model.Log{UserId: 1, Type: model.LogTypeVoice, Quota: 40, CreatedAt: createdAt + 3600}).Error)
						}
						stats, err := model.RebuildExpiriesFromDate(createdAt-1, map[int]int{model.LogTypeActive: 2})
						require.NoError(t, err)
						assert.Equal(t, 1, stats.RebuiltExpiryCount)
						assert.EqualValues(t, 60, stats.ProcessedExpiredVoidQuota)
						var user model.User
						require.NoError(t, model.DB.First(&user, 1).Error)
						assert.Zero(t, user.Quota)
						assert.Equal(t, 40, user.UsedQuota)
						var expiry model.LogQuotaExpiry
						require.NoError(t, model.DB.Where("log_id = ?", 1).First(&expiry).Error)
						assert.Equal(t, 40, expiry.ConsumedQuota)
						assert.Equal(t, model.LogQuotaExpiryStatusProcessed, expiry.Status)
						duplicate := expiry
						duplicate.Id = 0
						assert.Error(t, model.DB.Create(&duplicate).Error, "one expiry batch per source log")
						stats, err = model.RebuildExpiriesFromDate(createdAt-1, map[int]int{model.LogTypeActive: 2})
						require.NoError(t, err)
						assert.Zero(t, stats.ProcessedExpiredVoidQuota, "rebuilding must not deduct expired quota twice")
						var logs []model.Log
						require.NoError(t, model.LOG_DB.Where("type = ?", model.LogTypeQuotaExpiry).Find(&logs).Error)
						require.Len(t, logs, 1)
						assert.Equal(t, 60, logs[0].Quota)
						if upgrade {
							var historical model.Log
							require.NoError(t, model.LOG_DB.First(&historical, 2).Error)
							assert.Equal(t, "legacy voice", historical.Content)
							assert.Equal(t, model.LogTypeVoice, historical.Type)
						}
					})
				}
			}
		})
	}
}
