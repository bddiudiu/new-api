package controller

import (
	"errors"
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
	"gorm.io/gorm"
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
							require.NoError(t, db.Create(&releasedAuditUser{Username: "legacy-quota-owner", Password: "placeholder", AffCode: "quota-owner", Quota: 60}).Error)
							require.NoError(t, logDB.Create(&releasedAuditLog{UserId: 1, Type: model.LogTypeActive, Quota: 100, CreatedAt: createdAt, Content: "legacy activity"}).Error)
							require.NoError(t, logDB.Create(&releasedAuditLog{UserId: 1, Type: model.LogTypeVoice, Quota: 40, CreatedAt: createdAt + 3600, Content: "legacy voice"}).Error)
							require.NoError(t, db.AutoMigrate(&legacyQuotaExpiry{}))
							require.NoError(t, db.Create(&legacyQuotaExpiry{LogId: 1, UserId: 1, OriginalQuota: 100, ConsumedQuota: 40, CreatedAt: createdAt, ExpireAt: createdAt + 2*86400, Status: model.LogQuotaExpiryStatusPending}).Error)
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
							require.NoError(t, model.DB.Create(&model.User{Username: "fresh-quota-owner", Password: "placeholder", AffCode: "quota-owner", Quota: 60}).Error)
							require.NoError(t, model.LOG_DB.Create(&model.Log{UserId: 1, Type: model.LogTypeActive, Quota: 100, CreatedAt: createdAt}).Error)
							require.NoError(t, model.LOG_DB.Create(&model.Log{UserId: 1, Type: model.LogTypeVoice, Quota: 40, CreatedAt: createdAt + 3600}).Error)
						}
						require.NoError(t, model.DB.Model(&model.User{}).Where("id = ?", 1).Update("used_quota", 140).Error)
						stats, err := model.RebuildExpiriesFromDate(createdAt-1, map[int]int{model.LogTypeActive: 2})
						require.NoError(t, err)
						assert.Equal(t, 1, stats.RebuiltExpiryCount)
						assert.EqualValues(t, 60, stats.ProcessedExpiredVoidQuota)
						var user model.User
						require.NoError(t, model.DB.First(&user, 1).Error)
						assert.Zero(t, user.Quota)
						assert.Equal(t, 140, user.UsedQuota, "expiry rebuild must preserve accounting totals even when logs were pruned")
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
						verifyQuotaExpiryRegressions(t, createdAt, expiry)

					})
				}
			}
		})
	}
}

// Run the same accounting contracts against every real database and log split.
func verifyQuotaExpiryRegressions(t *testing.T, createdAt int64, processed model.LogQuotaExpiry) {
	t.Helper()
	previousLogging := common.LogConsumeEnabled
	common.LogConsumeEnabled = false
	t.Cleanup(func() { common.LogConsumeEnabled = previousLogging })

	// Removing and re-adding a rule must not erase the settled source identity.
	_, err := model.RebuildExpiriesFromDate(createdAt-1, nil)
	require.NoError(t, err)
	var preserved model.LogQuotaExpiry
	require.NoError(t, model.DB.First(&preserved, processed.Id).Error)
	assert.Equal(t, processed, preserved)
	_, err = model.RebuildExpiriesFromDate(createdAt-1, map[int]int{model.LogTypeActive: 7})
	require.NoError(t, err)
	require.NoError(t, model.DB.First(&preserved, processed.Id).Error)
	assert.Equal(t, processed, preserved, "settled amounts and dates are immutable")

	user := model.User{Username: "expiry-regression", AffCode: "expiry-reg", Quota: 1000, UsedQuota: 1234}
	require.NoError(t, model.DB.Create(&user).Error)
	model.RecordLogWithQuota(user.Id, model.LogTypeActive, 100, "grant")
	var batch model.LogQuotaExpiry
	require.NoError(t, model.DB.Where("user_id = ?", user.Id).First(&batch).Error)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	subscription := model.NewLogOther()
	subscription.SetPublic("billing_source", "subscription")
	model.RecordConsumeLog(ctx, user.Id, model.RecordConsumeLogParams{Quota: 90, Other: subscription})
	model.RecordConsumeLog(ctx, user.Id, model.RecordConsumeLogParams{Quota: 20, Content: "private request"})
	model.RecordTaskBillingLog(model.RecordTaskBillingLogParams{UserId: user.Id, LogType: model.LogTypeConsume, Quota: 10})
	model.RecordLogWithQuota(user.Id, model.LogTypeConsume, 5, "private external log")
	require.NoError(t, model.DB.First(&batch, batch.Id).Error)
	assert.Equal(t, 35, batch.ConsumedQuota, "wallet accounting survives disabled detailed logs")
	var accounting []model.Log
	require.NoError(t, model.LOG_DB.Where("user_id = ? AND type = ?", user.Id, model.LogTypeConsume).Find(&accounting).Error)
	require.Len(t, accounting, 3)
	for _, log := range accounting {
		assert.Empty(t, log.Content)
	}
	// Historical subscription logs must also be excluded during a rebuild.
	require.NoError(t, model.LOG_DB.Create(&model.Log{UserId: user.Id, Type: model.LogTypeConsume, Quota: 90, CreatedAt: batch.CreatedAt, Other: subscription.JSONString()}).Error)
	_, err = model.RebuildExpiriesFromDate(batch.CreatedAt, map[int]int{model.LogTypeActive: 2})
	require.NoError(t, err)
	require.NoError(t, model.DB.First(&batch, batch.Id).Error)
	assert.Equal(t, 35, batch.ConsumedQuota)
	var storedUser model.User
	require.NoError(t, model.DB.First(&storedUser, user.Id).Error)
	assert.Equal(t, 1234, storedUser.UsedQuota)
	voided, claimed, err := model.ProcessQuotaExpiry(batch.Id)
	require.NoError(t, err)
	assert.False(t, claimed, "future grants cannot be expired early")
	assert.Zero(t, voided)

	// Events written during a snapshot are replayed once, including with logs disabled.
	_, err = model.RebuildExpiriesFromDateWithProgress(batch.CreatedAt, map[int]int{model.LogTypeActive: 2}, func(stats model.RebuildQuotaExpiryStats) {
		if stats.Phase == "snapshot" {
			model.RecordConsumeLog(ctx, user.Id, model.RecordConsumeLogParams{Quota: 7})
		}
	})
	require.NoError(t, err)
	require.NoError(t, model.DB.First(&batch, batch.Id).Error)
	assert.Equal(t, 42, batch.ConsumedQuota)
	var oldLog model.Log
	require.NoError(t, model.LOG_DB.Where("user_id = ? AND type = ?", user.Id, model.LogTypeConsume).Order("id ASC").First(&oldLog).Error)
	require.NoError(t, model.HandleQuotaExpiryLog(&oldLog))
	require.NoError(t, model.DB.First(&batch, batch.Id).Error)
	assert.Equal(t, 42, batch.ConsumedQuota, "a late snapshot callback must not apply consumption twice")

	// A failed user rebuild must stop expiration and remain retryable.
	olderUser := model.User{Username: "expiry-older-only", AffCode: "expiry-older", Quota: 100}
	require.NoError(t, model.DB.Create(&olderUser).Error)
	olderGrant := model.Log{UserId: olderUser.Id, Type: model.LogTypeActive, Quota: 100, CreatedAt: batch.CreatedAt - 3600}
	require.NoError(t, model.LOG_DB.Create(&olderGrant).Error)
	require.NoError(t, model.HandleQuotaExpiryLog(&olderGrant))
	require.NoError(t, model.LOG_DB.Callback().Query().Before("gorm:query").Register("quota-expiry:fail-consume", func(tx *gorm.DB) {
		if tx.Statement.Table == "logs" && strings.Join(tx.Statement.Selects, ",") == "id,user_id,type,quota,created_at,other" {
			tx.AddError(errors.New("injected consumption read failure"))
		}
	}))
	_, err = model.RebuildExpiriesFromDate(batch.CreatedAt, map[int]int{model.LogTypeActive: 2})
	require.Error(t, err)
	require.NoError(t, model.LOG_DB.Callback().Query().Remove("quota-expiry:fail-consume"))
	state, err := model.GetQuotaExpiryRuntimeState()
	require.NoError(t, err)
	assert.Equal(t, model.QuotaExpiryRuntimeModeFailed, state.Mode)
	model.RecordConsumeLog(ctx, user.Id, model.RecordConsumeLogParams{Quota: 3})
	model.RecordConsumeLog(ctx, olderUser.Id, model.RecordConsumeLogParams{Quota: 7})
	_, err = model.RebuildExpiriesFromDate(batch.CreatedAt, map[int]int{model.LogTypeActive: 2})
	require.NoError(t, err)
	require.NoError(t, model.DB.First(&batch, batch.Id).Error)
	assert.Equal(t, 45, batch.ConsumedQuota)
	var olderBatch model.LogQuotaExpiry
	require.NoError(t, model.DB.Where("log_id = ?", olderGrant.Id).First(&olderBatch).Error)
	assert.Equal(t, 7, olderBatch.ConsumedQuota, "retry must retain queued consumption for grants before the selected start date")

	// A grant predating the selected date still absorbs its historical FIFO share.
	fifoUser := model.User{Username: "expiry-fifo", AffCode: "expiry-fifo", Quota: 1000}
	require.NoError(t, model.DB.Create(&fifoUser).Error)
	start := time.Now().AddDate(0, 0, -1).Unix()
	grant1 := model.Log{UserId: fifoUser.Id, Type: model.LogTypeActive, Quota: 100, CreatedAt: start - 3600}
	grant2 := model.Log{UserId: fifoUser.Id, Type: model.LogTypeActive, Quota: 80, CreatedAt: start + 1}
	require.NoError(t, model.LOG_DB.Create(&grant1).Error)
	require.NoError(t, model.HandleQuotaExpiryLog(&grant1))
	require.NoError(t, model.LOG_DB.Create(&grant2).Error)
	require.NoError(t, model.HandleQuotaExpiryLog(&grant2))
	for _, log := range []model.Log{
		{UserId: fifoUser.Id, Type: model.LogTypeVoice, Quota: 60, CreatedAt: start - 1},
		{UserId: fifoUser.Id, Type: model.LogTypeVoice, Quota: 50, CreatedAt: start + 2},
	} {
		require.NoError(t, model.LOG_DB.Create(&log).Error)
		require.NoError(t, model.HandleQuotaExpiryLog(&log))
	}
	_, err = model.RebuildExpiriesFromDate(start, map[int]int{model.LogTypeActive: 2})
	require.NoError(t, err)
	var fifoBatch model.LogQuotaExpiry
	require.NoError(t, model.DB.Where("log_id = ?", grant2.Id).First(&fifoBatch).Error)
	assert.Equal(t, 10, fifoBatch.ConsumedQuota)

	// A spend cannot consume a grant created later in the same second.
	sameSecondUser := model.User{Username: "expiry-same-second", AffCode: "expiry-second", Quota: 100}
	require.NoError(t, model.DB.Create(&sameSecondUser).Error)
	spend := model.Log{UserId: sameSecondUser.Id, Type: model.LogTypeVoice, Quota: 30, CreatedAt: start}
	laterGrant := model.Log{UserId: sameSecondUser.Id, Type: model.LogTypeActive, Quota: 100, CreatedAt: start}
	require.NoError(t, model.LOG_DB.Create(&spend).Error)
	require.NoError(t, model.LOG_DB.Create(&laterGrant).Error)
	require.NoError(t, model.HandleQuotaExpiryLog(&laterGrant))
	require.NoError(t, model.HandleQuotaExpiryLog(&spend))
	var sameSecondBatch model.LogQuotaExpiry
	require.NoError(t, model.DB.Where("log_id = ?", laterGrant.Id).First(&sameSecondBatch).Error)
	assert.Zero(t, sameSecondBatch.ConsumedQuota)
	_, err = model.RebuildExpiriesFromDate(start, map[int]int{model.LogTypeActive: 2})
	require.NoError(t, err)
	require.NoError(t, model.DB.First(&sameSecondBatch, sameSecondBatch.Id).Error)
	assert.Zero(t, sameSecondBatch.ConsumedQuota)

	// Expiration may remove available quota, but must never create wallet debt.
	require.NoError(t, model.DB.Model(&model.User{}).Where("id = ?", user.Id).Update("quota", 15).Error)
	require.NoError(t, model.DB.Model(&batch).Update("expire_at", createdAt).Error)
	voided, claimed, err = model.ProcessQuotaExpiry(batch.Id)
	require.NoError(t, err)
	assert.True(t, claimed)
	assert.Equal(t, 15, voided)
	require.NoError(t, model.DB.First(&storedUser, user.Id).Error)
	assert.Zero(t, storedUser.Quota)
	voided, claimed, err = model.ProcessQuotaExpiry(batch.Id)
	require.NoError(t, err)
	assert.False(t, claimed)
	assert.Zero(t, voided)
}

func TestQuotaExpiryRejectsInvalidRules(t *testing.T) {
	for _, value := range []string{
		`{}`, `null`, `[{"label":" ","log_type":9,"expire_days":2}]`,
		`[{"label":"Activity","log_type":9,"expire_days":0}]`,
		`[{"label":"Activity","log_type":9,"expire_days":999999999999}]`,
		`[{"label":"Activity","log_type":9,"expire_days":2},{"label":"Duplicate","log_type":9,"expire_days":7}]`,
		`[{"label":"Purchase","log_type":1,"expire_days":2}]`,
		`[{"label":"Consumption","log_type":2,"expire_days":2}]`,
	} {
		t.Run(value, func(t *testing.T) {
			body, err := common.Marshal(OptionUpdateRequest{Key: "quota_expiry_setting.rules", Value: value})
			require.NoError(t, err)
			response := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(response)
			ctx.Request = httptest.NewRequest(http.MethodPut, "/api/option/", strings.NewReader(string(body)))
			UpdateOption(ctx)
			var result struct {
				Success bool
				Message string
			}
			require.NoError(t, common.Unmarshal(response.Body.Bytes(), &result))
			assert.False(t, result.Success)
			assert.NotEmpty(t, result.Message)
		})
	}
	previousRules := operation_setting.GetQuotaExpirySetting().Rules
	t.Cleanup(func() { operation_setting.GetQuotaExpirySetting().Rules = previousRules })
	operation_setting.GetQuotaExpirySetting().Rules = []operation_setting.LogTypeExpiryRule{
		{Label: "First", LogType: model.LogTypeActive, ExpireDays: 2},
		{Label: "Legacy duplicate", LogType: model.LogTypeActive, ExpireDays: 7},
	}
	assert.Equal(t, operation_setting.GetExpireDaysForLogType(model.LogTypeActive), operation_setting.GetLogTypeExpireDaysMap()[model.LogTypeActive])
	require.NoError(t, model.ValidateQuotaExpiryRulesJSON(`[]`))
	require.NoError(t, model.ValidateQuotaExpiryRulesJSON(`[{"label":"签到","log_type":11,"expire_days":30}]`))
}
