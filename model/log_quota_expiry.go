package model

import (
	"fmt"
	"time"

	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

type LogQuotaExpiry struct {
	Id            int    `gorm:"primaryKey;autoIncrement"`
	LogId         int    `gorm:"uniqueIndex;not null"`
	UserId        int    `gorm:"index:idx_user_status_created,priority:1;not null"`
	OriginalQuota int    `gorm:"not null"`
	ConsumedQuota int    `gorm:"not null;default:0"`
	ExpireAt      int64  `gorm:"index:idx_expire_status,priority:1;not null"`
	Status        string `gorm:"type:varchar(16);default:'pending';index:idx_user_status_created,priority:2;index:idx_expire_status,priority:2"`
	CreatedAt     int64  `gorm:"index:idx_user_status_created,priority:3"`
}

const (
	LogQuotaExpiryStatusPending   = "pending"
	LogQuotaExpiryStatusProcessed = "processed"
)

func CreateLogQuotaExpiry(logId, userId, quota int, expireAt int64) error {
	expiry := &LogQuotaExpiry{
		LogId:         logId,
		UserId:        userId,
		OriginalQuota: quota,
		ConsumedQuota: 0,
		ExpireAt:      expireAt,
		Status:        LogQuotaExpiryStatusPending,
		CreatedAt:     common.GetTimestamp(),
	}
	return DB.Create(expiry).Error
}

// ApplyConsumeToExpiries 将本次消费额度按 FIFO 归因到各批次
func ApplyConsumeToExpiries(userId int, consumeQuota int) error {
	if consumeQuota <= 0 {
		return nil
	}

	// 查询该用户所有 pending 批次，按创建时间升序（FIFO）
	var batches []*LogQuotaExpiry
	err := DB.Where("user_id = ? AND status = ?", userId, LogQuotaExpiryStatusPending).
		Order("created_at ASC").
		Find(&batches).Error
	if err != nil {
		return err
	}

	remaining := consumeQuota
	for _, batch := range batches {
		if remaining <= 0 {
			break
		}

		// 计算本批次可填充额度
		available := batch.OriginalQuota - batch.ConsumedQuota
		if available <= 0 {
			continue
		}

		toConsume := remaining
		if toConsume > available {
			toConsume = available
		}

		// 乐观锁更新：WHERE consumed_quota = 旧值
		result := DB.Model(&LogQuotaExpiry{}).
			Where("id = ? AND consumed_quota = ?", batch.Id, batch.ConsumedQuota).
			Update("consumed_quota", batch.ConsumedQuota+toConsume)

		if result.Error != nil {
			common.SysError(fmt.Sprintf("failed to update consumed_quota for expiry %d: %v", batch.Id, result.Error))
			continue
		}

		if result.RowsAffected > 0 {
			remaining -= toConsume
		}
	}

	return nil
}

// GetPendingExpiredBatch 查询已到期的 pending 批次
func GetPendingExpiredBatch(batchSize int) ([]*LogQuotaExpiry, error) {
	var batches []*LogQuotaExpiry
	now := common.GetTimestamp()
	err := DB.Where("status = ? AND expire_at <= ?", LogQuotaExpiryStatusPending, now).
		Order("expire_at ASC").
		Limit(batchSize).
		Find(&batches).Error
	return batches, err
}

// MarkProcessed 标记批次为已处理
func MarkProcessed(id int) error {
	return DB.Model(&LogQuotaExpiry{}).
		Where("id = ?", id).
		Update("status", LogQuotaExpiryStatusProcessed).Error
}

// RebuildExpiriesFromDate 从指定日期开始全量重建 expiry 记录
func RebuildExpiriesFromDate(startTime int64, logTypeExpireDays map[int]int) error {
	// 1. 删除 startTime 之后的所有 pending expiry 记录
	err := DB.Where("created_at >= ? AND status = ?", startTime, LogQuotaExpiryStatusPending).
		Delete(&LogQuotaExpiry{}).Error
	if err != nil {
		return fmt.Errorf("failed to delete old expiries: %w", err)
	}

	// 2. 查询 startTime 之后所有符合配置的 log 记录
	var logs []*Log
	logTypes := make([]int, 0, len(logTypeExpireDays))
	for logType := range logTypeExpireDays {
		logTypes = append(logTypes, logType)
	}

	if len(logTypes) == 0 {
		return nil
	}

	err = LOG_DB.Where("created_at >= ? AND type IN ? AND quota > 0", startTime, logTypes).
		Order("created_at ASC").
		Find(&logs).Error
	if err != nil {
		return fmt.Errorf("failed to query logs: %w", err)
	}

	// 3. 为每条 log 创建 expiry 记录
	for _, log := range logs {
		days, ok := logTypeExpireDays[log.Type]
		if !ok || days <= 0 {
			continue
		}

		expireAt := time.Unix(log.CreatedAt, 0).AddDate(0, 0, days).Unix()
		err = CreateLogQuotaExpiry(log.Id, log.UserId, log.Quota, expireAt)
		if err != nil {
			common.SysLog(fmt.Sprintf("failed to create expiry for log %d: %v", log.Id, err))
		}
	}

	// 4. 重新计算所有用户的 consumed_quota
	// 查询所有受影响的用户
	var userIds []int
	err = DB.Model(&LogQuotaExpiry{}).
		Where("created_at >= ?", startTime).
		Distinct("user_id").
		Pluck("user_id", &userIds).Error
	if err != nil {
		return fmt.Errorf("failed to get affected users: %w", err)
	}

	// 对每个用户重新计算消费归因
	for _, userId := range userIds {
		err = rebuildUserConsumedQuota(userId, startTime)
		if err != nil {
			common.SysLog(fmt.Sprintf("failed to rebuild consumed_quota for user %d: %v", userId, err))
		}
	}

	return nil
}

// rebuildUserConsumedQuota 重新计算指定用户从 startTime 开始的消费归因
func rebuildUserConsumedQuota(userId int, startTime int64) error {
	return DB.Transaction(func(tx *gorm.DB) error {
		// 1. 重置该用户所有 expiry 的 consumed_quota
		err := tx.Model(&LogQuotaExpiry{}).
			Where("user_id = ? AND created_at >= ?", userId, startTime).
			Update("consumed_quota", 0).Error
		if err != nil {
			return err
		}

		// 2. 查询该用户从 startTime 开始的所有消费记录
		var consumeLogs []*Log
		err = tx.Where("user_id = ? AND type = ? AND created_at >= ? AND quota > 0", userId, LogTypeConsume, startTime).
			Order("created_at ASC").
			Find(&consumeLogs).Error
		if err != nil {
			return err
		}

		// 3. 按时间顺序 FIFO 归因
		for _, log := range consumeLogs {
			var batches []*LogQuotaExpiry
			err = tx.Where("user_id = ? AND status = ? AND created_at <= ?", userId, LogQuotaExpiryStatusPending, log.CreatedAt).
				Order("created_at ASC").
				Find(&batches).Error
			if err != nil {
				return err
			}

			remaining := log.Quota
			for _, batch := range batches {
				if remaining <= 0 {
					break
				}

				available := batch.OriginalQuota - batch.ConsumedQuota
				if available <= 0 {
					continue
				}

				toConsume := remaining
				if toConsume > available {
					toConsume = available
				}

				err = tx.Model(&LogQuotaExpiry{}).
					Where("id = ?", batch.Id).
					Update("consumed_quota", batch.ConsumedQuota+toConsume).Error
				if err != nil {
					return err
				}

				batch.ConsumedQuota += toConsume
				remaining -= toConsume
			}
		}

		return nil
	})
}
