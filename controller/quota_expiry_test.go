package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Wallet expiry contracts run against every configured database and log layout.
func TestQuotaExpiryScenarios(t *testing.T) {
	oldDB, oldLogDB := model.DB, model.LOG_DB
	oldMain, oldLog := common.MainDatabaseType(), common.LogDatabaseType()
	oldRules := operation_setting.GetQuotaExpirySetting().Rules
	oldRedis, oldLogging := common.RedisEnabled, common.LogConsumeEnabled
	common.RedisEnabled, common.LogConsumeEnabled = false, true
	operation_setting.GetQuotaExpirySetting().Rules = []operation_setting.LogTypeExpiryRule{{Label: "Activity", LogType: model.LogTypeActive, ExpireDays: 2}}
	t.Cleanup(func() {
		model.DB, model.LOG_DB = oldDB, oldLogDB
		common.SetDatabaseTypes(oldMain, oldLog)
		common.RedisEnabled, common.LogConsumeEnabled = oldRedis, oldLogging
		operation_setting.GetQuotaExpirySetting().Rules = oldRules
	})
	for _, engine := range []struct {
		name, env string
		kind      common.DatabaseType
	}{
		{"sqlite", "", common.DatabaseTypeSQLite}, {"mysql", "AUDIT_MYSQL_DSN", common.DatabaseTypeMySQL}, {"postgres", "AUDIT_POSTGRES_DSN", common.DatabaseTypePostgreSQL},
	} {
		t.Run(engine.name, func(t *testing.T) {
			dsn := os.Getenv(engine.env)
			if engine.env != "" && dsn == "" {
				t.Skip("real database DSN missing")
			}
			for _, split := range []bool{false, true} {
				t.Run(fmt.Sprintf("split=%v", split), func(t *testing.T) {
					db, _ := newAuditTestDatabase(t, engine.name, dsn)
					logDB := db
					if split {
						logDB, _ = newAuditTestDatabase(t, engine.name, dsn)
					}
					model.DB, model.LOG_DB = db, logDB
					common.SetDatabaseTypes(engine.kind, engine.kind)
					// Initialize dialect-specific quoted columns even when this suite
					// runs independently of startup/migration tests.
					t.Setenv("LOG_SQL_DSN", "")
					master := common.IsMasterNode
					common.IsMasterNode = false
					initErr := model.InitLogDB()
					common.IsMasterNode = master
					model.LOG_DB = logDB
					require.NoError(t, initErr)
					require.NoError(t, db.AutoMigrate(&model.User{}, &model.LogQuotaExpiry{}, &model.QuotaExpiryRuntimeState{}, &model.QuotaExpiryReplayLog{}))
					require.NoError(t, logDB.AutoMigrate(&model.Log{}, &model.QuotaExpiryLogOutbox{}))
					location := time.FixedZone("CST", 8*3600)
					now := time.Now().In(location)
					today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, location).Unix()
					for _, tc := range []struct {
						name                                   string
						consumed, wallet, wantVoid, wantWallet int
						due                                    bool
					}{
						{"unused", 0, 100, 100, 0, true}, {"partially_used", 40, 60, 60, 0, true}, {"fully_used", 100, 0, 0, 0, true},
						{"preserve_permanent_balance", 40, 260, 60, 200, true}, {"balance_below_expiry", 40, 15, 15, 0, true},
						{"zero_balance", 0, 0, 0, 0, true}, {"already_negative_balance", 0, -10, 0, -10, true}, {"not_due", 40, 60, 0, 60, false},
					} {
						t.Run(tc.name, func(t *testing.T) {
							created := today - 3*86400 + 3600
							if !tc.due {
								created = today + 1
							}
							user, grant, batch := newQuotaScenarioGrant(t, created, 100, tc.wallet)
							if tc.consumed > 0 {
								quotaScenarioSpend(t, user.Id, created+1, tc.consumed)
							}
							amount, processed, err := model.ProcessQuotaExpiry(batch.Id)
							require.NoError(t, err)
							assert.Equal(t, tc.due, processed)
							assert.Equal(t, tc.wantVoid, amount)
							stored, err := model.GetUserQuota(user.Id, true)
							require.NoError(t, err)
							assert.Equal(t, tc.wantWallet, stored)
							var actual model.LogQuotaExpiry
							require.NoError(t, db.Where("log_id = ?", grant.Id).First(&actual).Error)
							assert.Equal(t, tc.consumed, actual.ConsumedQuota)
						})
					}
					t.Run("fifo_spills_to_second_grant", func(t *testing.T) {
						user, _, first := newQuotaScenarioGrant(t, today+1, 100, 180)
						secondLog := model.Log{UserId: user.Id, Type: model.LogTypeActive, Quota: 80, CreatedAt: today + 2}
						require.NoError(t, logDB.Create(&secondLog).Error)
						require.NoError(t, model.HandleQuotaExpiryLog(&secondLog))
						quotaScenarioSpend(t, user.Id, today+3, 120)
						var second model.LogQuotaExpiry
						require.NoError(t, db.First(&first, first.Id).Error)
						require.NoError(t, db.Where("log_id = ?", secondLog.Id).First(&second).Error)
						assert.Equal(t, 100, first.ConsumedQuota)
						assert.Equal(t, 20, second.ConsumedQuota)
						quotaScenarioSpend(t, user.Id, today+4, 100)
						require.NoError(t, db.First(&second, second.Id).Error)
						assert.Equal(t, 80, second.ConsumedQuota)
					})
					t.Run("same_second_grant_before_spend", func(t *testing.T) {
						user, _, batch := newQuotaScenarioGrant(t, today+1, 100, 100)
						quotaScenarioSpend(t, user.Id, today+1, 30)
						require.NoError(t, db.First(&batch, batch.Id).Error)
						assert.Equal(t, 30, batch.ConsumedQuota)
						_, err := model.RebuildExpiriesFromDate(today, map[int]int{model.LogTypeActive: 2})
						require.NoError(t, err)
						require.NoError(t, db.First(&batch, batch.Id).Error)
						assert.Equal(t, 30, batch.ConsumedQuota)
					})
					t.Run("midnight_expiry_boundary", func(t *testing.T) {
						user, _, first := newQuotaScenarioGrant(t, today-2*86400+3600, 100, 500)
						secondLog := model.Log{UserId: user.Id, Type: model.LogTypeActive, Quota: 80, CreatedAt: today - 86400 + 1}
						require.NoError(t, logDB.Create(&secondLog).Error)
						require.NoError(t, model.HandleQuotaExpiryLog(&secondLog))
						quotaScenarioSpend(t, user.Id, today-1, 20)
						quotaScenarioSpend(t, user.Id, today, 50)
						var second model.LogQuotaExpiry
						require.NoError(t, db.First(&first, first.Id).Error)
						require.NoError(t, db.Where("log_id = ?", secondLog.Id).First(&second).Error)
						assert.Equal(t, 20, first.ConsumedQuota)
						assert.Equal(t, 50, second.ConsumedQuota)
						_, err := model.RebuildExpiriesFromDate(today-2*86400, map[int]int{model.LogTypeActive: 2})
						require.NoError(t, err)
						require.NoError(t, db.First(&first, first.Id).Error)
						require.NoError(t, db.First(&second, second.Id).Error)
						assert.Equal(t, 20, first.ConsumedQuota)
						assert.Equal(t, 50, second.ConsumedQuota)
					})
					t.Run("zero_negative_events_do_not_change_grants", func(t *testing.T) {
						user, _, batch := newQuotaScenarioGrant(t, today+1, 100, 100)
						for _, q := range []int{0, -1} {
							require.NoError(t, model.ApplyConsumeToExpiries(user.Id, q, today+2))
							model.RecordLogWithQuota(user.Id, model.LogTypeActive, q, "invalid grant")
						}
						require.NoError(t, db.First(&batch, batch.Id).Error)
						assert.Zero(t, batch.ConsumedQuota)
						var count int64
						require.NoError(t, db.Model(&model.LogQuotaExpiry{}).Where("user_id = ?", user.Id).Count(&count).Error)
						assert.EqualValues(t, 1, count)
					})
					t.Run("pending_rule_extension_preserves_consumption", func(t *testing.T) {
						user, grant, batch := newQuotaScenarioGrant(t, today+1, 100, 70)
						quotaScenarioSpend(t, user.Id, today+2, 30)
						_, err := model.RebuildExpiriesFromDate(today, map[int]int{model.LogTypeActive: 7})
						require.NoError(t, err)
						require.NoError(t, db.First(&batch, batch.Id).Error)
						assert.Equal(t, today+7*86400, batch.ExpireAt)
						assert.Equal(t, 30, batch.ConsumedQuota)
						var count int64
						require.NoError(t, db.Model(&model.LogQuotaExpiry{}).Where("log_id = ?", grant.Id).Count(&count).Error)
						assert.EqualValues(t, 1, count)
					})
					t.Run("source_changed_to_zero_is_removed", func(t *testing.T) {
						_, grant, batch := newQuotaScenarioGrant(t, today+1, 100, 100)
						require.NoError(t, logDB.Model(&grant).Update("quota", 0).Error)
						_, err := model.RebuildExpiriesFromDate(today, map[int]int{model.LogTypeActive: 2})
						require.NoError(t, err)
						var count int64
						require.NoError(t, db.Model(&model.LogQuotaExpiry{}).Where("id = ?", batch.Id).Count(&count).Error)
						assert.Zero(t, count)
					})
					t.Run("concurrent_consumption_is_not_lost", func(t *testing.T) {
						user, _, first := newQuotaScenarioGrant(t, today+1, 100, 180)
						secondLog := model.Log{UserId: user.Id, Type: model.LogTypeActive, Quota: 80, CreatedAt: today + 2}
						require.NoError(t, logDB.Create(&secondLog).Error)
						require.NoError(t, model.HandleQuotaExpiryLog(&secondLog))
						events := []model.Log{{UserId: user.Id, Type: model.LogTypeVoice, Quota: 70, CreatedAt: today + 3}, {UserId: user.Id, Type: model.LogTypeVoice, Quota: 60, CreatedAt: today + 3}}
						for i := range events {
							require.NoError(t, logDB.Create(&events[i]).Error)
						}
						start := make(chan struct{})
						results := make(chan error, 2)
						for i := range events {
							go func(log model.Log) { <-start; results <- model.HandleQuotaExpiryLog(&log) }(events[i])
						}
						close(start)
						for range events {
							require.NoError(t, <-results)
						}
						var second model.LogQuotaExpiry
						require.NoError(t, db.First(&first, first.Id).Error)
						require.NoError(t, db.Where("log_id = ?", secondLog.Id).First(&second).Error)
						assert.Equal(t, 100, first.ConsumedQuota)
						assert.Equal(t, 30, second.ConsumedQuota)
					})
					t.Run("concurrent_expiration_deducts_once", func(t *testing.T) {
						user, _, batch := newQuotaScenarioGrant(t, today-3*86400+1, 80, 100)
						quotaScenarioSpend(t, user.Id, today-3*86400+2, 20)
						type result struct {
							quota     int
							processed bool
							err       error
						}
						start := make(chan struct{})
						results := make(chan result, 2)
						for range 2 {
							go func() { <-start; q, p, e := model.ProcessQuotaExpiry(batch.Id); results <- result{q, p, e} }()
						}
						close(start)
						total, claims := 0, 0
						for range 2 {
							r := <-results
							require.NoError(t, r.err)
							total += r.quota
							if r.processed {
								claims++
							}
						}
						assert.Equal(t, 60, total)
						assert.Equal(t, 1, claims)
						balance, err := model.GetUserQuota(user.Id, true)
						require.NoError(t, err)
						assert.Equal(t, 40, balance)
					})
					t.Run("failed_deduction_rolls_back_and_retries", func(t *testing.T) {
						user, _, batch := newQuotaScenarioGrant(t, today-3*86400+1, 100, 100)
						callback := "quota-scenario:deduction"
						require.NoError(t, db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
							if tx.Statement.Table == "users" {
								tx.AddError(errors.New("injected debit failure"))
							}
						}))
						q, p, err := model.ProcessQuotaExpiry(batch.Id)
						require.Error(t, err)
						assert.Zero(t, q)
						assert.False(t, p)
						require.NoError(t, db.Callback().Update().Remove(callback))
						require.NoError(t, db.First(&batch, batch.Id).Error)
						assert.Equal(t, model.LogQuotaExpiryStatusPending, batch.Status)
						balance, err := model.GetUserQuota(user.Id, true)
						require.NoError(t, err)
						assert.Equal(t, 100, balance)
						q, p, err = model.ProcessQuotaExpiry(batch.Id)
						require.NoError(t, err)
						assert.True(t, p)
						assert.Equal(t, 100, q)
					})
					t.Run("duplicate_rebuild_is_rejected", func(t *testing.T) {
						_, _, _ = newQuotaScenarioGrant(t, today+1, 100, 100)
						_, err := model.RebuildExpiriesFromDateWithProgress(today, map[int]int{model.LogTypeActive: 2}, func(stats model.RebuildQuotaExpiryStats) {
							if stats.Phase == "snapshot" {
								_, otherErr := model.RebuildExpiriesFromDate(today, map[int]int{model.LogTypeActive: 2})
								assert.ErrorContains(t, otherErr, "already running")
							}
						})
						require.NoError(t, err)
					})
					t.Run("rebuild_with_one_database_connection", func(t *testing.T) {
						_, _, _ = newQuotaScenarioGrant(t, today+1, 100, 100)
						sqlDB, err := db.DB()
						require.NoError(t, err)
						previousMax := sqlDB.Stats().MaxOpenConnections
						sqlDB.SetMaxOpenConns(1)
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						model.DB = db.WithContext(ctx)
						model.LOG_DB = model.DB
						if split {
							model.LOG_DB = logDB.WithContext(ctx)
						}
						t.Cleanup(func() {
							cancel()
							model.DB, model.LOG_DB = db, logDB
							sqlDB.SetMaxOpenConns(previousMax)
						})
						_, err = model.RebuildExpiriesFromDate(today, map[int]int{model.LogTypeActive: 2})
						require.NoError(t, err, "a transaction must reuse its connection when reading a shared log table")
					})
					t.Run("corrected_grant_below_consumed_quota_never_credits_wallet", func(t *testing.T) {
						user, grant, _ := newQuotaScenarioGrant(t, today-3*86400+1, 100, 20)
						quotaScenarioSpend(t, user.Id, today-3*86400+2, 80)
						require.NoError(t, logDB.Model(&grant).Update("quota", 60).Error)
						stats, err := model.RebuildExpiriesFromDate(today-3*86400, map[int]int{model.LogTypeActive: 2})
						require.NoError(t, err)
						assert.Zero(t, stats.ProcessedExpiredVoidQuota)
						balance, err := model.GetUserQuota(user.Id, true)
						require.NoError(t, err)
						assert.Equal(t, 20, balance)
					})
					t.Run("failed_cache_invalidation_is_retried_without_another_debit", func(t *testing.T) {
						user, _, batch := newQuotaScenarioGrant(t, today-3*86400+1, 100, 100)
						server := miniredis.RunT(t)
						client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
						defer client.Close()
						oldRDB, enabled := common.RDB, common.RedisEnabled
						common.RDB, common.RedisEnabled = client, true
						t.Cleanup(func() { common.RDB, common.RedisEnabled = oldRDB, enabled })
						_, err := model.GetUserCache(user.Id)
						require.NoError(t, err)
						hook := &quotaRedisFailureHook{done: make(chan struct{}), failInvalidation: true}
						client.AddHook(hook)
						_, claimed, err := model.ProcessQuotaExpiry(batch.Id)
						require.Error(t, err)
						assert.True(t, claimed)
						require.NoError(t, db.First(&batch, batch.Id).Error)
						assert.True(t, batch.CachePending)
						hook.failInvalidation = false
						require.NoError(t, model.RecoverQuotaExpiryAccounting())
						require.NoError(t, model.RecoverQuotaExpiryAccounting())
						balance, err := model.GetUserQuota(user.Id, false)
						require.NoError(t, err)
						assert.Zero(t, balance)
						require.NoError(t, db.First(&batch, batch.Id).Error)
						assert.False(t, batch.CachePending)
					})
					t.Run("outbox_redelivery_after_later_rebuild_is_idempotent", func(t *testing.T) {
						if !split {
							t.Skip("requires independent log database")
						}
						user, _, batch := newQuotaScenarioGrant(t, today+1, 100, 100)
						callback := "quota-scenario:outbox-ack"
						require.NoError(t, logDB.Callback().Delete().Before("gorm:delete").Register(callback, func(tx *gorm.DB) {
							if tx.Statement.Table == "quota_expiry_log_outboxes" {
								tx.AddError(errors.New("injected acknowledgement failure"))
							}
						}))
						model.RecordLogWithQuota(user.Id, model.LogTypeConsume, 20, "committed before acknowledgement")
						require.NoError(t, logDB.Callback().Delete().Remove(callback))
						require.NoError(t, db.First(&batch, batch.Id).Error)
						require.Equal(t, 20, batch.ConsumedQuota)
						_, err := model.RebuildExpiriesFromDate(today+86400, map[int]int{model.LogTypeActive: 2})
						require.NoError(t, err)
						require.NoError(t, model.RecoverQuotaExpiryAccounting())
						require.NoError(t, model.RecoverQuotaExpiryAccounting())
						require.NoError(t, db.First(&batch, batch.Id).Error)
						assert.Equal(t, 20, batch.ConsumedQuota, "rebuilding from a later date must not forget a delivered event")
						var pending int64
						require.NoError(t, logDB.Model(&model.QuotaExpiryLogOutbox{}).Count(&pending).Error)
						assert.Zero(t, pending)
					})
					t.Run("void_log_ack_failure_does_not_duplicate_audit", func(t *testing.T) {
						if !split {
							t.Skip("requires independent log database")
						}
						user, _, batch := newQuotaScenarioGrant(t, today-3*86400+1, 100, 100)
						callback := "quota-scenario:void-ack"
						require.NoError(t, db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
							if tx.Statement.Table == "log_quota_expiries" {
								if values, ok := tx.Statement.Dest.(map[string]interface{}); ok {
									if _, ok := values["void_log_id"]; ok {
										tx.AddError(errors.New("injected audit acknowledgement failure"))
									}
								}
							}
						}))
						q, claimed, err := model.ProcessQuotaExpiry(batch.Id)
						require.Error(t, err)
						assert.True(t, claimed)
						assert.Equal(t, 100, q)
						require.NoError(t, db.Callback().Update().Remove(callback))
						require.NoError(t, model.RecoverQuotaExpiryAccounting())
						require.NoError(t, model.RecoverQuotaExpiryAccounting())
						var logs []model.Log
						require.NoError(t, logDB.Where("user_id = ? AND type = ?", user.Id, model.LogTypeQuotaExpiry).Find(&logs).Error)
						require.Len(t, logs, 1)
						assert.Equal(t, 100, logs[0].Quota)
						balance, err := model.GetUserQuota(user.Id, true)
						require.NoError(t, err)
						assert.Zero(t, balance)
					})
					t.Run("replaced_rebuild_cannot_overwrite_new_owner", func(t *testing.T) {
						_, _, _ = newQuotaScenarioGrant(t, today+1, 100, 100)
						_, err := model.RebuildExpiriesFromDateWithProgressAndJobID(today, map[int]int{model.LogTypeActive: 2}, "stalled-owner", func(stats model.RebuildQuotaExpiryStats) {
							if stats.Phase != "snapshot" {
								return
							}
							require.NoError(t, db.Model(&model.QuotaExpiryRuntimeState{}).Where("job_id = ?", "stalled-owner").UpdateColumn("updated_at", today-86400).Error)
							_, err := model.RebuildExpiriesFromDateWithProgressAndJobID(today, map[int]int{model.LogTypeActive: 2}, "replacement", nil)
							require.NoError(t, err)
						})
						require.ErrorContains(t, err, "lease lost")
						state, err := model.GetQuotaExpiryRuntimeState()
						require.NoError(t, err)
						assert.Equal(t, model.QuotaExpiryRuntimeModeNormal, state.Mode)
					})
					t.Run("log_insert_failure_has_no_attribution", func(t *testing.T) {
						user, _, batch := newQuotaScenarioGrant(t, today+1, 100, 100)
						callback := "quota-scenario:log-insert"
						require.NoError(t, logDB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
							if tx.Statement.Table == "logs" {
								tx.AddError(errors.New("injected log failure"))
							}
						}))
						model.RecordLogWithQuota(user.Id, model.LogTypeConsume, 20, "failed log")
						require.NoError(t, logDB.Callback().Create().Remove(callback))
						require.NoError(t, db.First(&batch, batch.Id).Error)
						assert.Zero(t, batch.ConsumedQuota)
						var count int64
						require.NoError(t, logDB.Model(&model.Log{}).Where("user_id = ? AND type = ?", user.Id, model.LogTypeConsume).Count(&count).Error)
						assert.Zero(t, count)
					})
					{
						t.Run("deleted_last_source_is_cleaned", func(t *testing.T) {
							_, grant, batch := newQuotaScenarioGrant(t, today+1, 100, 100)
							require.NoError(t, logDB.Delete(&grant).Error)
							_, err := model.RebuildExpiriesFromDate(today, map[int]int{model.LogTypeActive: 2})
							require.NoError(t, err)
							var count int64
							require.NoError(t, db.Model(&model.LogQuotaExpiry{}).Where("id = ?", batch.Id).Count(&count).Error)
							assert.Zero(t, count, "deleted latest source must not leave an expiring orphan")
						})
						t.Run("pruned_consumption_preserves_permanent_balance", func(t *testing.T) {
							// The wallet contains 200 permanent + 100 gift quota. Debit a
							// real 60 before retention removes logs; cleanup is not a refund.
							user, _, batch := newQuotaScenarioGrant(t, today-10*86400+1, 100, 300)
							quotaScenarioSpend(t, user.Id, today-10*86400+2, 60)
							require.NoError(t, db.Model(&model.User{}).Where("id = ?", user.Id).Updates(map[string]any{"quota": gorm.Expr("quota - ?", 60), "used_quota": 60}).Error)
							_, err := model.DeleteOldLogBatch(context.Background(), today-9*86400, 100)
							require.NoError(t, err)
							balance, err := model.GetUserQuota(user.Id, true)
							require.NoError(t, err)
							require.Equal(t, 240, balance, "the production log cleanup must not reverse a settled debit")
							model.RecordLog(user.Id, model.LogTypeSystem, "retention completed")
							_, err = model.RebuildExpiriesFromDate(today-8*86400, map[int]int{model.LogTypeActive: 2})
							require.NoError(t, err)
							require.NoError(t, db.First(&batch, batch.Id).Error)
							assert.Equal(t, 60, batch.ConsumedQuota)
							voided, processed, err := model.ProcessQuotaExpiry(batch.Id)
							require.NoError(t, err)
							assert.True(t, processed)
							assert.Equal(t, 40, voided, "only the unspent gift can expire")
							balance, err = model.GetUserQuota(user.Id, true)
							require.NoError(t, err)
							assert.Equal(t, 200, balance, "log retention must preserve permanent wallet funds")
						})
						t.Run("void_log_failure_is_recoverable", func(t *testing.T) {
							user, _, _ := newQuotaScenarioGrant(t, today-3*86400+1, 100, 100)
							callback := "quota-scenario:void-log"
							require.NoError(t, logDB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
								if log, ok := tx.Statement.Dest.(*model.Log); ok && log.Type == model.LogTypeQuotaExpiry {
									tx.AddError(errors.New("injected void log failure"))
								}
							}))
							_, firstErr := model.RebuildExpiriesFromDate(today-3*86400, map[int]int{model.LogTypeActive: 2})
							require.Error(t, firstErr)
							var failed model.LogQuotaExpiry
							require.NoError(t, db.Where("user_id = ?", user.Id).First(&failed).Error)
							if split {
								assert.Equal(t, model.LogQuotaExpiryStatusProcessed, failed.Status)
								assert.Equal(t, 100, failed.VoidQuota)
								assert.Zero(t, failed.VoidLogID)
							} else {
								assert.Equal(t, model.LogQuotaExpiryStatusPending, failed.Status)
							}
							require.NoError(t, logDB.Callback().Create().Remove(callback))
							require.NoError(t, model.RecoverQuotaExpiryAccounting())
							_, err := model.RebuildExpiriesFromDate(today-3*86400, map[int]int{model.LogTypeActive: 2})
							require.NoError(t, err)
							balance, err := model.GetUserQuota(user.Id, true)
							require.NoError(t, err)
							var logs []model.Log
							require.NoError(t, logDB.Where("user_id = ? AND type = ?", user.Id, model.LogTypeQuotaExpiry).Find(&logs).Error)
							assert.Zero(t, balance, "the successful retry must finish expiration")
							assert.Len(t, logs, 1, "a completed debit must retain one recoverable audit log")
						})
						t.Run("attribution_failure_is_recoverable", func(t *testing.T) {
							user, _, batch := newQuotaScenarioGrant(t, today+1, 100, 100)
							callback := "quota-scenario:attribution"
							require.NoError(t, db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
								if tx.Statement.Table == "log_quota_expiries" {
									tx.AddError(errors.New("injected attribution failure"))
								}
							}))
							model.RecordLogWithQuota(user.Id, model.LogTypeConsume, 20, "attribution failure")
							require.NoError(t, db.Callback().Update().Remove(callback))
							require.NoError(t, db.First(&batch, batch.Id).Error)
							assert.Zero(t, batch.ConsumedQuota)
							var count int64
							require.NoError(t, logDB.Model(&model.Log{}).Where("user_id = ? AND type = ?", user.Id, model.LogTypeConsume).Count(&count).Error)
							if split {
								assert.EqualValues(t, 1, count, "split logs retain a durable event for automatic retry")
							} else {
								assert.Zero(t, count, "shared database rolls back the source log")
							}
							require.NoError(t, model.RecoverQuotaExpiryAccounting())
							require.NoError(t, model.RecoverQuotaExpiryAccounting())
							require.NoError(t, db.First(&batch, batch.Id).Error)
							if split {
								assert.Equal(t, 20, batch.ConsumedQuota, "automatic redelivery attributes the source log exactly once")
								var pending int64
								require.NoError(t, logDB.Model(&model.QuotaExpiryLogOutbox{}).Where("user_id = ?", user.Id).Count(&pending).Error)
								assert.Zero(t, pending)
							}
							balance, err := model.GetUserQuota(user.Id, true)
							require.NoError(t, err)
							assert.Equal(t, 100, balance, "attribution recovery must not debit the wallet again")
						})
						t.Run("abandoned_rebuild_can_restart", func(t *testing.T) {
							state, err := model.GetQuotaExpiryRuntimeState()
							require.NoError(t, err)
							saved := *state
							t.Cleanup(func() { require.NoError(t, db.Save(&saved).Error) })
							state.Mode = model.QuotaExpiryRuntimeModeRebuilding
							state.JobId = "dead-process"
							state.StartTime = today
							state.UpdatedAt = today - 86400
							require.NoError(t, db.Model(state).Select("mode", "job_id", "start_time", "updated_at").UpdateColumns(state).Error)
							_, err = model.RebuildExpiriesFromDate(today, map[int]int{model.LogTypeActive: 2})
							assert.NoError(t, err, "a persisted rebuilding marker from a dead process needs a recovery path")
						})
						t.Run("redis_delta_failure_does_not_serve_expired_balance", func(t *testing.T) {
							user, _, batch := newQuotaScenarioGrant(t, today-3*86400+1, 100, 100)
							server := miniredis.RunT(t)
							client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
							defer client.Close()
							oldRDB, enabled := common.RDB, common.RedisEnabled
							common.RDB, common.RedisEnabled = client, true
							t.Cleanup(func() { common.RDB, common.RedisEnabled = oldRDB, enabled })
							// Populate through the actual cache loader, then reject only the delta script.
							_, err := model.GetUserCache(user.Id)
							require.NoError(t, err)
							hook := &quotaRedisFailureHook{done: make(chan struct{})}
							client.AddHook(hook)
							q, p, err := model.ProcessQuotaExpiry(batch.Id)
							require.NoError(t, err)
							assert.True(t, p)
							assert.Equal(t, 100, q)
							select {
							case <-hook.done:
							case <-time.After(5 * time.Second):
								t.Fatal("cache delta did not finish")
							}
							balance, err := model.GetUserQuota(user.Id, false)
							require.NoError(t, err)
							assert.Zero(t, balance, "cache reads must not keep the expired grant spendable after a delta failure")
						})
					}
				})
			}
		})
	}
}

func newQuotaScenarioGrant(t *testing.T, createdAt int64, quota, wallet int) (model.User, model.Log, model.LogQuotaExpiry) {
	t.Helper()
	var count int64
	require.NoError(t, model.DB.Model(&model.User{}).Count(&count).Error)
	user := model.User{Username: fmt.Sprintf("qe-%d", count+1), AffCode: fmt.Sprintf("qe-%d", count+1), Quota: wallet, AuthVersion: 1}
	require.NoError(t, model.DB.Create(&user).Error)
	log := model.Log{UserId: user.Id, Type: model.LogTypeActive, Quota: quota, CreatedAt: createdAt}
	require.NoError(t, model.LOG_DB.Create(&log).Error)
	require.NoError(t, model.HandleQuotaExpiryLog(&log))
	var batch model.LogQuotaExpiry
	require.NoError(t, model.DB.Where("log_id = ?", log.Id).First(&batch).Error)
	return user, log, batch
}
func quotaScenarioSpend(t *testing.T, userID int, createdAt int64, quota int) model.Log {
	t.Helper()
	log := model.Log{UserId: userID, Type: model.LogTypeVoice, Quota: quota, CreatedAt: createdAt}
	require.NoError(t, model.LOG_DB.Create(&log).Error)
	require.NoError(t, model.HandleQuotaExpiryLog(&log))
	return log
}

type quotaRedisFailureHook struct {
	failInvalidation bool
	done             chan struct{}
	once             sync.Once
}

func (h *quotaRedisFailureHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	if cmd.Name() == "del" && h.failInvalidation {
		return ctx, errors.New("injected cache invalidation failure")
	}
	if (cmd.Name() == "evalsha" || cmd.Name() == "eval") && strings.Contains(fmt.Sprint(cmd.Args()), "-100") {
		h.once.Do(func() { close(h.done) })
		return ctx, errors.New("injected quota cache script failure")
	}
	return ctx, nil
}
func (h *quotaRedisFailureHook) AfterProcess(context.Context, redis.Cmder) error { return nil }
func (h *quotaRedisFailureHook) BeforeProcessPipeline(ctx context.Context, _ []redis.Cmder) (context.Context, error) {
	return ctx, nil
}
func (h *quotaRedisFailureHook) AfterProcessPipeline(context.Context, []redis.Cmder) error {
	return nil
}

func TestQuotaExpiryAPIValidationScenarios(t *testing.T) {
	for _, body := range []string{`{}`, `{"start_date":"bad"}`, `{"start_date":"2026-02-30"}`} {
		t.Run(body, func(t *testing.T) {
			r := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(r)
			c.Request = httptest.NewRequest(http.MethodPost, "/api/quota-expiry/rebuild", strings.NewReader(body))
			RebuildQuotaExpiry(c)
			assert.Equal(t, http.StatusBadRequest, r.Code)
		})
	}
	for _, tc := range []struct {
		query string
		want  int
	}{{"", 400}, {"?task_id=unknown-selftest", 404}} {
		t.Run(tc.query, func(t *testing.T) {
			r := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(r)
			c.Request = httptest.NewRequest(http.MethodGet, "/api/quota-expiry/rebuild/status"+tc.query, nil)
			GetRebuildStatus(c)
			assert.Equal(t, tc.want, r.Code)
		})
	}
}

// Schema of quota batches before recoverable expiry settlement was added.
type legacyQuotaExpiry struct {
	Id            int `gorm:"primaryKey;autoIncrement"`
	LogId         int `gorm:"uniqueIndex;not null"`
	UserId        int `gorm:"not null"`
	OriginalQuota int `gorm:"not null"`
	ConsumedQuota int `gorm:"not null;default:0"`
	ExpireAt      int64
	Status        string `gorm:"type:varchar(16);default:'pending'"`
	CreatedAt     int64
}

func (legacyQuotaExpiry) TableName() string { return "log_quota_expiries" }
