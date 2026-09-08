package model

import (
	"fmt"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"gorm.io/gorm"
)

const quotaExpiryRebuildLeaseSeconds int64 = 5 * 60

// Every rebuild mutation fences its owner under the same lock used to acquire
// a lease. A stalled worker cannot overwrite a replacement rebuild's results.
func quotaExpiryRebuildTransaction(jobID string, apply func(*gorm.DB) error) error {
	return DB.Transaction(func(tx *gorm.DB) error {
		state, err := lockQuotaExpiryRuntimeState(tx)
		if err != nil {
			return err
		}
		if state.Mode != QuotaExpiryRuntimeModeRebuilding || state.JobId != jobID {
			return fmt.Errorf("quota expiry rebuild lease lost: job_id=%s", jobID)
		}
		if err := apply(tx); err != nil {
			return err
		}
		return tx.Model(state).Update("updated_at", common.GetTimestamp()).Error
	})
}

// The source log and this outbox commit together in LOG_DB. Main-database
// receipts make redelivery harmless even if acknowledging the outbox fails.
func deliverQuotaExpiryLogOutbox(tx *gorm.DB, state *QuotaExpiryRuntimeState) ([]int, error) {
	var delivered []int
	lastLogID := 0
	for {
		var events []QuotaExpiryLogOutbox
		if err := LOG_DB.Where("log_id > ?", lastLogID).Order("log_id ASC").Limit(quotaExpiryReplayBatchSize).Find(&events).Error; err != nil {
			return nil, err
		}
		for _, event := range events {
			log := &Log{Id: event.LogId, UserId: event.UserId, Type: event.LogType, Quota: event.Quota, CreatedAt: event.LogCreatedAt}
			if err := handleQuotaExpiryLogWithState(tx, state, log); err != nil {
				return nil, err
			}
			delivered = append(delivered, event.LogId)
			lastLogID = event.LogId
		}
		if len(events) < quotaExpiryReplayBatchSize {
			return delivered, nil
		}
	}
}

func acknowledgeQuotaExpiryLogOutbox(logIDs []int) error {
	for len(logIDs) > 0 {
		n := min(len(logIDs), quotaExpiryRebuildUpdateBatchSize)
		if err := LOG_DB.Where("log_id IN ?", logIDs[:n]).Delete(&QuotaExpiryLogOutbox{}).Error; err != nil {
			return err
		}
		logIDs = logIDs[n:]
	}
	return nil
}

func quotaExpiryVoidLog(tx *gorm.DB, expiry *LogQuotaExpiry) (*Log, error) {
	var user User
	if err := tx.Unscoped().First(&user, expiry.UserId).Error; err != nil {
		return nil, err
	}
	return &Log{
		UserId: expiry.UserId, Username: user.Username, Group: user.Group,
		Type: LogTypeQuotaExpiry, Quota: expiry.VoidQuota, CreatedAt: expiry.ProcessedAt,
		Content:   fmt.Sprintf("有效期到期-%d额度作废", expiry.VoidQuota),
		RequestId: fmt.Sprintf("quota_expiry_%d", expiry.Id),
	}, nil
}

// VoidQuota and ProcessedAt are committed with the debit. In a split layout,
// publishing the log is retried independently and never repeats the debit.
func FlushQuotaExpiryVoidLogs() error {
	for {
		var pending []LogQuotaExpiry
		if err := DB.Where("status = ? AND void_quota > 0 AND void_log_id = 0", LogQuotaExpiryStatusProcessed).
			Order("id ASC").Limit(quotaExpiryRebuildUpdateBatchSize).Find(&pending).Error; err != nil {
			return err
		}
		for _, candidate := range pending {
			if err := DB.Transaction(func(tx *gorm.DB) error {
				if _, err := lockQuotaExpiryRuntimeState(tx); err != nil {
					return err
				}
				var expiry LogQuotaExpiry
				if err := lockForUpdate(tx).First(&expiry, candidate.Id).Error; err != nil {
					return err
				}
				if expiry.VoidLogID != 0 {
					return nil
				}
				log, err := quotaExpiryVoidLog(tx, &expiry)
				if err != nil {
					return err
				}
				logDB := LOG_DB
				if LOG_DB == DB {
					logDB = tx
				}
				var existing Log
				err = logDB.Where("request_id = ? AND type = ?", log.RequestId, LogTypeQuotaExpiry).First(&existing).Error
				if err != nil && err != gorm.ErrRecordNotFound {
					return err
				}
				if err == gorm.ErrRecordNotFound {
					if err := logDB.Create(log).Error; err != nil {
						return err
					}
				} else {
					log.Id = existing.Id
				}
				logID := log.Id
				if common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
					// ClickHouse uses request_id as its log identity.
					logID = -1
				}
				return tx.Model(&expiry).Update("void_log_id", logID).Error
			}); err != nil {
				return fmt.Errorf("failed to publish expiry %d audit log: %w", candidate.Id, err)
			}
		}
		if len(pending) < quotaExpiryRebuildUpdateBatchSize {
			return nil
		}
	}
}

// Serialize cache completion with recovery. Always invalidate after the delta:
// a concurrent reader may have hydrated the already-debited database value in
// the interval after commit, so applying only a delta could double-subtract it.
func syncQuotaExpiryCache(id int, applyDelta bool) error {
	if !common.RedisEnabled {
		return nil
	}
	return DB.Transaction(func(tx *gorm.DB) error {
		if _, err := lockQuotaExpiryRuntimeState(tx); err != nil {
			return err
		}
		var expiry LogQuotaExpiry
		if err := tx.First(&expiry, id).Error; err != nil {
			return err
		}
		if !expiry.CachePending {
			return nil
		}
		if applyDelta {
			if err := cacheDecrUserQuota(expiry.UserId, int64(expiry.VoidQuota)); err != nil {
				common.SysError("failed to decrease expiry quota cache: " + err.Error())
			}
		}
		if err := invalidateUserCache(expiry.UserId); err != nil {
			return err
		}
		return tx.Model(&expiry).Update("cache_pending", false).Error
	})
}

// Called on startup and by the periodic task, even when no new grants expire.
func RecoverQuotaExpiryAccounting() error {
	// Cache deltas are not safe to repeat after an interrupted commit. A
	// persisted pending marker is instead recovered by invalidation.
	if common.RedisEnabled {
		for {
			var pending []LogQuotaExpiry
			if err := DB.Where("cache_pending = ?", true).Limit(quotaExpiryRebuildUpdateBatchSize).Find(&pending).Error; err != nil {
				return err
			}
			for _, expiry := range pending {
				if err := syncQuotaExpiryCache(expiry.Id, false); err != nil {
					return err
				}
			}
			if len(pending) < quotaExpiryRebuildUpdateBatchSize {
				break
			}
		}
	}
	if err := FlushQuotaExpiryVoidLogs(); err != nil {
		return err
	}
	state, err := GetQuotaExpiryRuntimeState()
	if err != nil {
		return err
	}
	if state.Mode == QuotaExpiryRuntimeModeFailed ||
		(state.Mode == QuotaExpiryRuntimeModeRebuilding && common.GetTimestamp()-state.UpdatedAt >= quotaExpiryRebuildLeaseSeconds) {
		_, err := RebuildExpiriesFromDate(state.StartTime, operation_setting.GetLogTypeExpireDaysMap())
		return err
	}
	if LOG_DB == DB || common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
		return nil
	}
	var delivered []int
	err = DB.Transaction(func(tx *gorm.DB) error {
		state, err := lockQuotaExpiryRuntimeState(tx)
		if err != nil {
			return err
		}
		delivered, err = deliverQuotaExpiryLogOutbox(tx, state)
		return err
	})
	if err != nil {
		return err
	}
	return acknowledgeQuotaExpiryLogOutbox(delivered)
}
