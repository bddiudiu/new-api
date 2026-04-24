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

type RebuildQuotaExpiryStats struct {
	RebuiltExpiryCount        int   `json:"rebuilt_expiry_count"`
	AffectedUserCount         int   `json:"affected_user_count"`
	ProcessedExpiredCount     int   `json:"processed_expired_count"`
	ProcessedExpiredVoidQuota int64 `json:"processed_expired_void_quota"`
	GeneratedExpiryLogCount   int   `json:"generated_expiry_log_count"`
	RestoredProcessedCount    int   `json:"restored_processed_count"`
}

const (
	LogQuotaExpiryStatusPending    = "pending"
	LogQuotaExpiryStatusProcessing = "processing"
	LogQuotaExpiryStatusProcessed  = "processed"
)

var quotaExpiryLocation = func() *time.Location {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.FixedZone("CST", 8*3600)
	}
	return location
}()

func quotaExpiryDayStart(ts int64) int64 {
	if ts <= 0 {
		return 0
	}
	t := time.Unix(ts, 0).In(quotaExpiryLocation)
	dayStart := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, quotaExpiryLocation)
	return dayStart.Unix()
}

func quotaExpiryDateOnly(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.Unix(ts, 0).In(quotaExpiryLocation).Format("2006-01-02")
}

func calculateQuotaExpireAt(createdAt int64, expireDays int) int64 {
	if createdAt <= 0 || expireDays <= 0 {
		return 0
	}
	createdDay := time.Unix(createdAt, 0).In(quotaExpiryLocation)
	expireDayStart := time.Date(createdDay.Year(), createdDay.Month(), createdDay.Day(), 0, 0, 0, 0, quotaExpiryLocation).AddDate(0, 0, expireDays)
	return expireDayStart.Unix()
}

func CreateLogQuotaExpiry(logId, userId, quota int, expireAt int64) error {
	return CreateLogQuotaExpiryWithCreatedAt(logId, userId, quota, expireAt, 0)
}

func CreateLogQuotaExpiryWithCreatedAt(logId, userId, quota int, expireAt int64, createdAt int64) error {
	if createdAt <= 0 {
		createdAt = common.GetTimestamp()
	}
	expiry := &LogQuotaExpiry{
		LogId:         logId,
		UserId:        userId,
		OriginalQuota: quota,
		ConsumedQuota: 0,
		ExpireAt:      expireAt,
		Status:        LogQuotaExpiryStatusPending,
		CreatedAt:     createdAt,
	}
	return DB.Create(expiry).Error
}

// ApplyConsumeToExpiries 将本次消费额度按 FIFO 归因到各批次
func ApplyConsumeToExpiries(userId int, consumeQuota int, consumeAt int64) error {
	return applyConsumeToExpiriesWithDB(DB, userId, consumeQuota, consumeAt)
}

func applyConsumeToExpiriesWithDB(db *gorm.DB, userId int, consumeQuota int, consumeAt int64) error {
	if consumeQuota <= 0 {
		return nil
	}
	if consumeAt <= 0 {
		consumeAt = common.GetTimestamp()
	}

	remaining := consumeQuota
	conflicts := 0
	for remaining > 0 {
		var batch LogQuotaExpiry
		err := db.Where(
			"user_id = ? AND status = ? AND created_at <= ? AND original_quota > consumed_quota",
			userId,
			LogQuotaExpiryStatusPending,
			consumeAt,
		).
			Order("created_at ASC, id ASC").
			First(&batch).Error
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		available := batch.OriginalQuota - batch.ConsumedQuota
		if available <= 0 {
			continue
		}
		toConsume := remaining
		if toConsume > available {
			toConsume = available
		}
		result := db.Model(&LogQuotaExpiry{}).
			Where(
				"id = ? AND status = ? AND consumed_quota = ? AND original_quota - consumed_quota >= ?",
				batch.Id,
				LogQuotaExpiryStatusPending,
				batch.ConsumedQuota,
				toConsume,
			).
			Update("consumed_quota", gorm.Expr("consumed_quota + ?", toConsume))
		if result.Error != nil {
			common.SysError(fmt.Sprintf("failed to update consumed_quota for expiry %d: %v", batch.Id, result.Error))
			return result.Error
		}
		if result.RowsAffected == 0 {
			conflicts++
			if conflicts >= 1024 {
				return fmt.Errorf("too many concurrent conflicts while applying consume for user %d", userId)
			}
			continue
		}
		conflicts = 0
		remaining -= toConsume
	}
	return nil
}

// GetPendingExpiredBatch 查询已到期的 pending 批次
func GetPendingExpiredBatch(batchSize int) ([]*LogQuotaExpiry, error) {
	var batches []*LogQuotaExpiry
	currentDayStart := quotaExpiryDayStart(common.GetTimestamp())
	err := DB.Where("status = ? AND expire_at <= ?", LogQuotaExpiryStatusPending, currentDayStart).
		Order("expire_at ASC, id ASC").
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

func ProcessQuotaExpiry(id int) (int, bool, error) {
	return processQuotaExpiry(id, "quota_expiry")
}

func ProcessQuotaExpiryWithReason(id int, reason string) (int, bool, error) {
	return processQuotaExpiry(id, reason)
}

func processQuotaExpiry(id int, reason string) (int, bool, error) {
	voidQuota := 0
	userId := 0
	expireAt := int64(0)
	claimed := false

	err := DB.Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&LogQuotaExpiry{}).
			Where("id = ? AND status = ?", id, LogQuotaExpiryStatusPending).
			Update("status", LogQuotaExpiryStatusProcessing)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		claimed = true

		var expiry LogQuotaExpiry
		if err := tx.Where("id = ?", id).First(&expiry).Error; err != nil {
			return err
		}
		userId = expiry.UserId
		expireAt = expiry.ExpireAt
		voidQuota = expiry.OriginalQuota - expiry.ConsumedQuota
		if voidQuota > 0 {
			if err := tx.Model(&User{}).
				Where("id = ?", expiry.UserId).
				Update("quota", gorm.Expr("quota - ?", voidQuota)).Error; err != nil {
				return err
			}
		}
		return tx.Model(&LogQuotaExpiry{}).
			Where("id = ?", id).
			Update("status", LogQuotaExpiryStatusProcessed).Error
	})
	if err != nil {
		return 0, false, err
	}
	if !claimed {
		return 0, false, nil
	}
	if voidQuota > 0 && common.RedisEnabled {
		go func() {
			if err := cacheDecrUserQuota(userId, int64(voidQuota)); err != nil {
				common.SysLog("failed to decrease user quota cache: " + err.Error())
			}
		}()
	}
	common.SysLog(fmt.Sprintf("quota expiry processed: reason=%s expiry_id=%d user_id=%d expire_date=%s void_quota=%d", reason, id, userId, quotaExpiryDateOnly(expireAt), voidQuota))
	return voidQuota, true, nil
}

func getRebuildExpiryStatusByLogID(startTime int64) (map[int]string, error) {
	var expiries []*LogQuotaExpiry
	err := DB.Where("created_at >= ?", startTime).Find(&expiries).Error
	if err != nil {
		return nil, err
	}

	statusByLogID := make(map[int]string, len(expiries))
	for _, expiry := range expiries {
		statusByLogID[expiry.LogId] = expiry.Status
	}
	return statusByLogID, nil
}

func restoreProcessedExpiryStatus(startTime int64, statusByLogID map[int]string) (int, error) {
	processedLogIDs := make([]int, 0, len(statusByLogID))
	for logID, status := range statusByLogID {
		if status == LogQuotaExpiryStatusProcessed {
			processedLogIDs = append(processedLogIDs, logID)
		}
	}
	if len(processedLogIDs) == 0 {
		return 0, nil
	}

	result := DB.Model(&LogQuotaExpiry{}).
		Where("created_at >= ? AND log_id IN ?", startTime, processedLogIDs).
		Update("status", LogQuotaExpiryStatusProcessed)
	if result.Error != nil {
		return 0, result.Error
	}
	return int(result.RowsAffected), nil
}

func rebuildUserUsedQuota(userId int) error {
	var total int64
	err := LOG_DB.Model(&Log{}).
		Where("user_id = ? AND type = ?", userId, LogTypeConsume).
		Select("COALESCE(SUM(quota), 0)").
		Scan(&total).Error
	if err != nil {
		return err
	}

	return DB.Model(&User{}).
		Where("id = ?", userId).
		Update("used_quota", int(total)).Error
}

func processExpiredExpiriesFromDate(startTime int64, statusByLogID map[int]string) (*RebuildQuotaExpiryStats, error) {
	stats := &RebuildQuotaExpiryStats{}
	var pendingExpiries []*LogQuotaExpiry
	err := DB.Where(
		"created_at >= ? AND status = ?",
		startTime,
		LogQuotaExpiryStatusPending,
	).
		Order("expire_at ASC, id ASC").
		Find(&pendingExpiries).Error
	if err != nil {
		return nil, err
	}

	currentDayStart := quotaExpiryDayStart(common.GetTimestamp())
	common.SysLog(fmt.Sprintf("quota expiry rebuild: scanning expired pending batches from start_time=%d, count=%d, current_date=%s", startTime, len(pendingExpiries), quotaExpiryDateOnly(currentDayStart)))
	for _, expiry := range pendingExpiries {
		if statusByLogID[expiry.LogId] == LogQuotaExpiryStatusProcessed {
			continue
		}
		if expiry.ExpireAt > currentDayStart {
			continue
		}
		voidQuota, processed, err := ProcessQuotaExpiryWithReason(expiry.Id, "rebuild_init")
		if err != nil {
			return nil, err
		}
		if !processed {
			continue
		}
		stats.ProcessedExpiredCount++
		stats.ProcessedExpiredVoidQuota += int64(voidQuota)
		if voidQuota <= 0 {
			common.SysLog(fmt.Sprintf("quota expiry rebuild: expiry_id=%d user_id=%d already fully consumed before expire_date=%s", expiry.Id, expiry.UserId, quotaExpiryDateOnly(expiry.ExpireAt)))
			continue
		}
		RecordQuotaExpiryVoidLog(expiry.UserId, voidQuota)
		common.SysLog(fmt.Sprintf("quota expiry rebuild: expiry_id=%d user_id=%d expired quota deducted, expire_date=%s, void_quota=%d", expiry.Id, expiry.UserId, quotaExpiryDateOnly(expiry.ExpireAt), voidQuota))
		stats.GeneratedExpiryLogCount++
	}
	common.SysLog(fmt.Sprintf("quota expiry rebuild completed: start_time=%d processed=%d voided=%d logs=%d", startTime, stats.ProcessedExpiredCount, stats.ProcessedExpiredVoidQuota, stats.GeneratedExpiryLogCount))
	return stats, nil
}

func RecordQuotaExpiryVoidLog(userId int, voidQuota int) {
	if voidQuota <= 0 {
		return
	}
	content := fmt.Sprintf("有效期到期-%d额度作废", voidQuota)
	recordLogWithQuota(userId, LogTypeQuotaExpiry, voidQuota, content, false)
}

func canConsumeFromExpiry(expiry *LogQuotaExpiry, consumeAt int64) bool {
	if consumeAt < expiry.CreatedAt {
		return false
	}
	return quotaExpiryDayStart(consumeAt) < expiry.ExpireAt
}

// RebuildExpiriesFromDate 从指定日期开始全量重建 expiry 记录
func RebuildExpiriesFromDate(startTime int64, logTypeExpireDays map[int]int) (*RebuildQuotaExpiryStats, error) {
	stats := &RebuildQuotaExpiryStats{}

	// 1. 读取旧状态，便于重建后恢复已处理记录，避免重复扣减用户额度
	statusByLogID, err := getRebuildExpiryStatusByLogID(startTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query old expiries: %w", err)
	}

	// 2. 删除 startTime 之后的所有 expiry 记录，完整重建状态
	err = DB.Where("created_at >= ?", startTime).
		Delete(&LogQuotaExpiry{}).Error
	if err != nil {
		return nil, fmt.Errorf("failed to delete old expiries: %w", err)
	}

	// 3. 查询 startTime 之后所有符合配置的 log 记录
	var logs []*Log
	logTypes := make([]int, 0, len(logTypeExpireDays))
	for logType := range logTypeExpireDays {
		logTypes = append(logTypes, logType)
	}

	if len(logTypes) == 0 {
		return stats, nil
	}

	err = LOG_DB.Where("created_at >= ? AND type IN ? AND quota > 0", startTime, logTypes).
		Order("created_at ASC").
		Find(&logs).Error
	if err != nil {
		return nil, fmt.Errorf("failed to query logs: %w", err)
	}

	userSet := make(map[int]struct{})

	// 4. 为每条 log 创建 expiry 记录
	for _, log := range logs {
		days, ok := logTypeExpireDays[log.Type]
		if !ok || days <= 0 {
			continue
		}

		userSet[log.UserId] = struct{}{}
		expireAt := calculateQuotaExpireAt(log.CreatedAt, days)
		err = CreateLogQuotaExpiryWithCreatedAt(log.Id, log.UserId, log.Quota, expireAt, log.CreatedAt)
		if err != nil {
			common.SysLog(fmt.Sprintf("failed to create expiry for log %d: %v", log.Id, err))
			continue
		}
		stats.RebuiltExpiryCount++
	}

	for logID := range statusByLogID {
		var userId int
		err = LOG_DB.Model(&Log{}).
			Where("id = ?", logID).
			Select("user_id").
			Scan(&userId).Error
		if err != nil {
			return nil, fmt.Errorf("failed to get user for old expiry log %d: %w", logID, err)
		}
		if userId > 0 {
			userSet[userId] = struct{}{}
		}
	}

	userIds := make([]int, 0, len(userSet))
	for userId := range userSet {
		userIds = append(userIds, userId)
	}
	stats.AffectedUserCount = len(userIds)

	// 5. 重新计算所有受影响用户的 consumed_quota 与 used_quota
	for _, userId := range userIds {
		err = rebuildUserConsumedQuota(userId, startTime)
		if err != nil {
			common.SysLog(fmt.Sprintf("failed to rebuild consumed_quota for user %d: %v", userId, err))
		}
		err = rebuildUserUsedQuota(userId)
		if err != nil {
			common.SysLog(fmt.Sprintf("failed to rebuild used_quota for user %d: %v", userId, err))
		}
	}

	// 6. 恢复历史上已处理的状态，避免重复扣减用户额度
	stats.RestoredProcessedCount, err = restoreProcessedExpiryStatus(startTime, statusByLogID)
	if err != nil {
		return nil, fmt.Errorf("failed to restore processed expiries: %w", err)
	}

	// 7. 对历史上未处理、但当前已过期的批次补做过期处理
	expiredStats, err := processExpiredExpiriesFromDate(startTime, statusByLogID)
	if err != nil {
		return nil, fmt.Errorf("failed to process expired expiries: %w", err)
	}
	stats.ProcessedExpiredCount = expiredStats.ProcessedExpiredCount
	stats.ProcessedExpiredVoidQuota = expiredStats.ProcessedExpiredVoidQuota
	stats.GeneratedExpiryLogCount = expiredStats.GeneratedExpiryLogCount

	return stats, nil
}

// rebuildUserConsumedQuota 重新计算指定用户从 startTime 开始的消费归因
func rebuildUserConsumedQuota(userId int, startTime int64) error {
	var consumeLogs []*Log
	err := LOG_DB.Where("user_id = ? AND type = ? AND created_at >= ? AND quota > 0", userId, LogTypeConsume, startTime).
		Order("created_at ASC, id ASC").
		Find(&consumeLogs).Error
	if err != nil {
		return err
	}

	return DB.Transaction(func(tx *gorm.DB) error {
		// 1. 读取并重置该用户所有重建范围内的 expiry。
		var expiries []*LogQuotaExpiry
		err := tx.Where("user_id = ? AND created_at >= ?", userId, startTime).
			Order("created_at ASC, id ASC").
			Find(&expiries).Error
		if err != nil {
			return err
		}

		err = tx.Model(&LogQuotaExpiry{}).
			Where("user_id = ? AND created_at >= ?", userId, startTime).
			Update("consumed_quota", 0).Error
		if err != nil {
			return err
		}

		// 2. 按时间顺序 FIFO 归因，但批次只允许在其历史有效窗口内被消费。
		for _, log := range consumeLogs {
			remaining := log.Quota
			for _, expiry := range expiries {
				if remaining == 0 {
					break
				}
				if !canConsumeFromExpiry(expiry, log.CreatedAt) {
					continue
				}

				available := expiry.OriginalQuota - expiry.ConsumedQuota
				if available <= 0 {
					continue
				}
				toConsume := remaining
				if toConsume > available {
					toConsume = available
				}
				expiry.ConsumedQuota += toConsume
				remaining -= toConsume
			}
		}

		// 3. 回写重建后的 consumed_quota。
		for _, expiry := range expiries {
			err := tx.Model(&LogQuotaExpiry{}).
				Where("id = ?", expiry.Id).
				Update("consumed_quota", expiry.ConsumedQuota).Error
			if err != nil {
				return err
			}
		}

		return nil
	})
}
