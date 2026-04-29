package model

import (
	"fmt"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LogQuotaExpiry struct {
	Id            int    `gorm:"primaryKey;autoIncrement;index:idx_lqe_user_created_id,priority:3;index:idx_lqe_status_expire_id,priority:3"`
	LogId         int    `gorm:"uniqueIndex;not null"`
	UserId        int    `gorm:"index:idx_user_status_created,priority:1;index:idx_lqe_user_created_id,priority:1;not null"`
	OriginalQuota int    `gorm:"not null"`
	ConsumedQuota int    `gorm:"not null;default:0"`
	ExpireAt      int64  `gorm:"index:idx_expire_status,priority:1;index:idx_lqe_status_expire_id,priority:2;not null"`
	Status        string `gorm:"type:varchar(16);default:'pending';index:idx_user_status_created,priority:2;index:idx_expire_status,priority:2;index:idx_lqe_status_expire_id,priority:1"`
	CreatedAt     int64  `gorm:"index:idx_user_status_created,priority:3;index:idx_lqe_user_created_id,priority:2"`
}

type QuotaExpiryRuntimeState struct {
	Id               int    `gorm:"primaryKey;autoIncrement:false"`
	Mode             string `gorm:"type:varchar(16);not null;default:'normal';index"`
	JobId            string `gorm:"type:varchar(64);not null;default:'';index"`
	StartTime        int64  `gorm:"index"`
	SnapshotMaxLogID int
	UpdatedAt        int64
}

type QuotaExpiryReplayLog struct {
	Id           int    `gorm:"primaryKey;autoIncrement"`
	JobId        string `gorm:"type:varchar(64);not null;default:'';index"`
	LogId        int    `gorm:"uniqueIndex;not null"`
	UserId       int    `gorm:"index;not null"`
	LogType      int    `gorm:"index:idx_qerl_status_created_id,priority:3;not null"`
	Quota        int    `gorm:"not null"`
	LogCreatedAt int64  `gorm:"index:idx_qerl_status_created_id,priority:2;not null"`
	Status       string `gorm:"type:varchar(16);not null;default:'pending';index:idx_qerl_status_created_id,priority:1"`
	CreatedAt    int64
}

type RebuildQuotaExpiryStats struct {
	RebuiltExpiryCount        int    `json:"rebuilt_expiry_count"`
	AffectedUserCount         int    `json:"affected_user_count"`
	ProcessedUserCount        int    `json:"processed_user_count"`
	FailedUserCount           int    `json:"failed_user_count"`
	ProcessedExpiredCount     int    `json:"processed_expired_count"`
	ProcessedExpiredVoidQuota int64  `json:"processed_expired_void_quota"`
	GeneratedExpiryLogCount   int    `json:"generated_expiry_log_count"`
	ScannedLogCount           int    `json:"scanned_log_count"`
	DeletedStaleCount         int    `json:"deleted_stale_count"`
	UpdatedConsumedCount      int    `json:"updated_consumed_count"`
	ReplayedLogCount          int    `json:"replayed_log_count"`
	ReplayPendingCount        int64  `json:"replay_pending_count"`
	SnapshotMaxLogID          int    `json:"snapshot_max_log_id"`
	CurrentLogID              int    `json:"current_log_id,omitempty"`
	CurrentUserID             int    `json:"current_user_id,omitempty"`
	Phase                     string `json:"phase,omitempty"`
}

type RebuildQuotaExpiryProgress func(stats RebuildQuotaExpiryStats)

const (
	LogQuotaExpiryStatusPending    = "pending"
	LogQuotaExpiryStatusProcessing = "processing"
	LogQuotaExpiryStatusProcessed  = "processed"

	QuotaExpiryRuntimeModeNormal     = "normal"
	QuotaExpiryRuntimeModeRebuilding = "rebuilding"

	QuotaExpiryReplayStatusPending   = "pending"
	QuotaExpiryReplayStatusProcessed = "processed"

	quotaExpiryRuntimeStateID = 1

	quotaExpiryRebuildLogBatchSize    = 5000
	quotaExpiryRebuildUserBatchSize   = 200
	quotaExpiryRebuildUpdateBatchSize = 500
	quotaExpiryReplayBatchSize        = 1000
	quotaExpiryReplayPreDrainBatches  = 20
)

var quotaExpiryLocation = func() *time.Location {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.FixedZone("CST", 8*3600)
	}
	return location
}()

func ensureQuotaExpiryRuntimeState(tx *gorm.DB) error {
	state := &QuotaExpiryRuntimeState{
		Id:        quotaExpiryRuntimeStateID,
		Mode:      QuotaExpiryRuntimeModeNormal,
		UpdatedAt: common.GetTimestamp(),
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(state).Error
}

func lockQuotaExpiryRuntimeState(tx *gorm.DB) (*QuotaExpiryRuntimeState, error) {
	if err := ensureQuotaExpiryRuntimeState(tx); err != nil {
		return nil, err
	}

	if common.UsingSQLite {
		err := tx.Model(&QuotaExpiryRuntimeState{}).
			Where("id = ?", quotaExpiryRuntimeStateID).
			UpdateColumn("updated_at", gorm.Expr("updated_at")).Error
		if err != nil {
			return nil, err
		}
	}

	var state QuotaExpiryRuntimeState
	query := tx.Where("id = ?", quotaExpiryRuntimeStateID)
	if !common.UsingSQLite {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := query.First(&state).Error; err != nil {
		return nil, err
	}
	return &state, nil
}

func GetQuotaExpiryRuntimeState() (*QuotaExpiryRuntimeState, error) {
	var state QuotaExpiryRuntimeState
	err := DB.Where("id = ?", quotaExpiryRuntimeStateID).First(&state).Error
	if err == nil {
		return &state, nil
	}
	if err == gorm.ErrRecordNotFound {
		return &QuotaExpiryRuntimeState{
			Id:        quotaExpiryRuntimeStateID,
			Mode:      QuotaExpiryRuntimeModeNormal,
			UpdatedAt: common.GetTimestamp(),
		}, nil
	}
	return nil, err
}

func QuotaExpiryRebuildRunning() (bool, error) {
	state, err := GetQuotaExpiryRuntimeState()
	if err != nil {
		return false, err
	}
	return state.Mode == QuotaExpiryRuntimeModeRebuilding, nil
}

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

func HandleQuotaExpiryLog(log *Log) error {
	if !isQuotaExpiryTrackedLog(log) {
		return nil
	}
	if log.CreatedAt <= 0 {
		log.CreatedAt = common.GetTimestamp()
	}

	return DB.Transaction(func(tx *gorm.DB) error {
		state, err := lockQuotaExpiryRuntimeState(tx)
		if err != nil {
			return err
		}

		if state.Mode != QuotaExpiryRuntimeModeRebuilding {
			return applyQuotaExpiryLogWithDB(tx, log)
		}
		if log.CreatedAt < state.StartTime || log.Id <= 0 {
			return applyQuotaExpiryLogWithDB(tx, log)
		}
		if log.Id <= state.SnapshotMaxLogID {
			return nil
		}
		return enqueueQuotaExpiryReplayLog(tx, state.JobId, log)
	})
}

func isQuotaExpiryTrackedLog(log *Log) bool {
	if log == nil || log.UserId <= 0 || log.Quota <= 0 {
		return false
	}
	if IsQuotaConsumeLogType(log.Type) {
		return true
	}
	return operation_setting.GetExpireDaysForLogType(log.Type) > 0
}

func applyQuotaExpiryLogWithDB(db *gorm.DB, log *Log) error {
	if !isQuotaExpiryTrackedLog(log) {
		return nil
	}
	if log.CreatedAt <= 0 {
		log.CreatedAt = common.GetTimestamp()
	}
	if IsQuotaConsumeLogType(log.Type) {
		return applyConsumeToExpiriesWithDB(db, log.UserId, log.Quota, log.CreatedAt)
	}

	days := operation_setting.GetExpireDaysForLogType(log.Type)
	if days <= 0 {
		return nil
	}
	if log.Id <= 0 {
		return fmt.Errorf("quota expiry log id is required for log type %d", log.Type)
	}
	expiry := &LogQuotaExpiry{
		LogId:         log.Id,
		UserId:        log.UserId,
		OriginalQuota: log.Quota,
		ConsumedQuota: 0,
		ExpireAt:      calculateQuotaExpireAt(log.CreatedAt, days),
		Status:        LogQuotaExpiryStatusPending,
		CreatedAt:     log.CreatedAt,
	}
	return upsertLogQuotaExpiryBatchWithDB(db, []*LogQuotaExpiry{expiry})
}

func enqueueQuotaExpiryReplayLog(tx *gorm.DB, jobID string, log *Log) error {
	replayLog := &QuotaExpiryReplayLog{
		JobId:        jobID,
		LogId:        log.Id,
		UserId:       log.UserId,
		LogType:      log.Type,
		Quota:        log.Quota,
		LogCreatedAt: log.CreatedAt,
		Status:       QuotaExpiryReplayStatusPending,
		CreatedAt:    common.GetTimestamp(),
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "log_id"}},
		DoNothing: true,
	}).Create(replayLog).Error
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
	consumeDayStart := quotaExpiryDayStart(consumeAt)
	for remaining > 0 {
		var batch LogQuotaExpiry
		err := db.Where(
			"user_id = ? AND status = ? AND created_at <= ? AND expire_at > ? AND original_quota > consumed_quota",
			userId,
			LogQuotaExpiryStatusPending,
			consumeAt,
			consumeDayStart,
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
		state, err := lockQuotaExpiryRuntimeState(tx)
		if err != nil {
			return err
		}
		if state.Mode == QuotaExpiryRuntimeModeRebuilding {
			return nil
		}

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

func processExpiredExpiriesFromDate(startTime int64) (*RebuildQuotaExpiryStats, error) {
	stats := &RebuildQuotaExpiryStats{}
	currentDayStart := quotaExpiryDayStart(common.GetTimestamp())
	for {
		var pendingExpiries []*LogQuotaExpiry
		err := DB.Where(
			"created_at >= ? AND status = ? AND expire_at <= ?",
			startTime,
			LogQuotaExpiryStatusPending,
			currentDayStart,
		).
			Order("expire_at ASC, id ASC").
			Limit(quotaExpiryRebuildUpdateBatchSize).
			Find(&pendingExpiries).Error
		if err != nil {
			return nil, err
		}
		if len(pendingExpiries) == 0 {
			break
		}

		for _, expiry := range pendingExpiries {
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
				continue
			}
			RecordQuotaExpiryVoidLog(expiry.UserId, voidQuota)
			stats.GeneratedExpiryLogCount++
		}

		if len(pendingExpiries) < quotaExpiryRebuildUpdateBatchSize {
			break
		}
	}
	return stats, nil
}

func RecordQuotaExpiryVoidLog(userId int, voidQuota int) {
	if voidQuota <= 0 {
		return
	}
	content := fmt.Sprintf("有效期到期-%d额度作废", voidQuota)
	recordLogWithQuota(userId, LogTypeQuotaExpiry, voidQuota, content, false)
}

// RebuildExpiriesFromDate 从指定日期开始按快照重建 expiry 记录。
func RebuildExpiriesFromDate(startTime int64, logTypeExpireDays map[int]int) (*RebuildQuotaExpiryStats, error) {
	return RebuildExpiriesFromDateWithProgress(startTime, logTypeExpireDays, nil)
}

func RebuildExpiriesFromDateWithProgress(startTime int64, logTypeExpireDays map[int]int, progress RebuildQuotaExpiryProgress) (*RebuildQuotaExpiryStats, error) {
	jobID := fmt.Sprintf("rebuild_%d", time.Now().UnixNano())
	return RebuildExpiriesFromDateWithProgressAndJobID(startTime, logTypeExpireDays, jobID, progress)
}

func RebuildExpiriesFromDateWithProgressAndJobID(startTime int64, logTypeExpireDays map[int]int, jobID string, progress RebuildQuotaExpiryProgress) (*RebuildQuotaExpiryStats, error) {
	if jobID == "" {
		jobID = fmt.Sprintf("rebuild_%d", time.Now().UnixNano())
	}
	stats := &RebuildQuotaExpiryStats{}
	report := func(phase string) {
		stats.Phase = phase
		if progress != nil {
			progress(*stats)
		}
	}

	snapshotMaxLogID, err := beginQuotaExpiryRebuild(startTime, jobID)
	if err != nil {
		return nil, err
	}
	stats.SnapshotMaxLogID = snapshotMaxLogID
	report("snapshot")

	logTypes := logTypesFromExpireDays(logTypeExpireDays)
	if len(logTypes) > 0 {
		if err := rebuildExpiriesFromLogs(startTime, snapshotMaxLogID, logTypeExpireDays, logTypes, stats, report); err != nil {
			return nil, err
		}
	}

	report("cleaning_stale")
	deleted, err := cleanupStaleExpiriesFromDate(startTime, snapshotMaxLogID, logTypes)
	if err != nil {
		return nil, err
	}
	stats.DeletedStaleCount = deleted
	report("rebuilding_users")

	if err := rebuildAffectedUsersFromDate(startTime, snapshotMaxLogID, stats, report); err != nil {
		return nil, err
	}

	report("replaying_logs")
	if err := drainQuotaExpiryReplayLogs(jobID, stats, quotaExpiryReplayPreDrainBatches, report); err != nil {
		return nil, fmt.Errorf("failed to replay quota expiry logs: %w", err)
	}

	report("finalizing")
	if err := finishQuotaExpiryRebuild(jobID, stats); err != nil {
		return nil, fmt.Errorf("failed to finalize quota expiry rebuild: %w", err)
	}

	report("processing_expired")
	expiredStats, err := processExpiredExpiriesFromDate(startTime)
	if err != nil {
		return nil, fmt.Errorf("failed to process expired expiries: %w", err)
	}
	stats.ProcessedExpiredCount = expiredStats.ProcessedExpiredCount
	stats.ProcessedExpiredVoidQuota = expiredStats.ProcessedExpiredVoidQuota
	stats.GeneratedExpiryLogCount = expiredStats.GeneratedExpiryLogCount
	report("completed")

	return stats, nil
}

func beginQuotaExpiryRebuild(startTime int64, jobID string) (int, error) {
	snapshotMaxLogID := 0
	err := DB.Transaction(func(tx *gorm.DB) error {
		state, err := lockQuotaExpiryRuntimeState(tx)
		if err != nil {
			return err
		}
		if state.Mode == QuotaExpiryRuntimeModeRebuilding {
			return fmt.Errorf("quota expiry rebuild already running: job_id=%s snapshot_max_log_id=%d", state.JobId, state.SnapshotMaxLogID)
		}

		snapshotDB := LOG_DB
		if LOG_DB == DB {
			snapshotDB = tx
		}
		snapshot, err := getQuotaExpiryRebuildSnapshotMaxLogIDWithDB(snapshotDB)
		if err != nil {
			return fmt.Errorf("failed to query rebuild snapshot: %w", err)
		}

		state.Mode = QuotaExpiryRuntimeModeRebuilding
		state.JobId = jobID
		state.StartTime = startTime
		state.SnapshotMaxLogID = snapshot
		state.UpdatedAt = common.GetTimestamp()
		if err := tx.Save(state).Error; err != nil {
			return err
		}
		snapshotMaxLogID = snapshot
		return nil
	})
	return snapshotMaxLogID, err
}

func finishQuotaExpiryRebuild(jobID string, stats *RebuildQuotaExpiryStats) error {
	replayed := 0
	err := DB.Transaction(func(tx *gorm.DB) error {
		state, err := lockQuotaExpiryRuntimeState(tx)
		if err != nil {
			return err
		}
		if state.Mode != QuotaExpiryRuntimeModeRebuilding || state.JobId != jobID {
			return fmt.Errorf("quota expiry rebuild state changed: mode=%s job_id=%s", state.Mode, state.JobId)
		}

		for {
			processed, err := drainQuotaExpiryReplayLogBatchWithDB(tx, jobID)
			if err != nil {
				return err
			}
			replayed += processed
			if processed < quotaExpiryReplayBatchSize {
				break
			}
		}

		state.Mode = QuotaExpiryRuntimeModeNormal
		state.JobId = ""
		state.StartTime = 0
		state.SnapshotMaxLogID = 0
		state.UpdatedAt = common.GetTimestamp()
		return tx.Save(state).Error
	})
	if err != nil {
		return err
	}
	if stats != nil {
		stats.ReplayedLogCount += replayed
		stats.ReplayPendingCount = 0
	}
	return nil
}

func drainQuotaExpiryReplayLogs(jobID string, stats *RebuildQuotaExpiryStats, maxBatches int, report func(string)) error {
	for batches := 0; maxBatches <= 0 || batches < maxBatches; batches++ {
		processed := 0
		err := DB.Transaction(func(tx *gorm.DB) error {
			var err error
			processed, err = drainQuotaExpiryReplayLogBatchWithDB(tx, jobID)
			return err
		})
		if err != nil {
			return err
		}
		if processed == 0 {
			break
		}
		if stats != nil {
			stats.ReplayedLogCount += processed
			pending, err := countQuotaExpiryReplayPending(jobID)
			if err != nil {
				return err
			}
			stats.ReplayPendingCount = pending
		}
		if report != nil {
			report("replaying_logs")
		}
		if processed < quotaExpiryReplayBatchSize {
			break
		}
	}
	return nil
}

func drainQuotaExpiryReplayLogBatchWithDB(tx *gorm.DB, jobID string) (int, error) {
	var replayLogs []*QuotaExpiryReplayLog
	err := tx.
		Where("job_id = ? AND status = ?", jobID, QuotaExpiryReplayStatusPending).
		Order("log_created_at ASC, log_id ASC").
		Limit(quotaExpiryReplayBatchSize).
		Find(&replayLogs).Error
	if err != nil {
		return 0, err
	}
	if len(replayLogs) == 0 {
		return 0, nil
	}

	processed := 0
	for _, replayLog := range replayLogs {
		log := &Log{
			Id:        replayLog.LogId,
			UserId:    replayLog.UserId,
			CreatedAt: replayLog.LogCreatedAt,
			Type:      replayLog.LogType,
			Quota:     replayLog.Quota,
		}
		if err := applyQuotaExpiryLogWithDB(tx, log); err != nil {
			return processed, fmt.Errorf("failed to apply replay log %d: %w", replayLog.LogId, err)
		}
		result := tx.Model(&QuotaExpiryReplayLog{}).
			Where("id = ? AND status = ?", replayLog.Id, QuotaExpiryReplayStatusPending).
			Update("status", QuotaExpiryReplayStatusProcessed)
		if result.Error != nil {
			return processed, result.Error
		}
		processed += int(result.RowsAffected)
	}
	return processed, nil
}

func countQuotaExpiryReplayPending(jobID string) (int64, error) {
	var count int64
	err := DB.Model(&QuotaExpiryReplayLog{}).
		Where("job_id = ? AND status = ?", jobID, QuotaExpiryReplayStatusPending).
		Count(&count).Error
	return count, err
}

func getQuotaExpiryRebuildSnapshotMaxLogID() (int, error) {
	return getQuotaExpiryRebuildSnapshotMaxLogIDWithDB(LOG_DB)
}

func getQuotaExpiryRebuildSnapshotMaxLogIDWithDB(db *gorm.DB) (int, error) {
	var maxID int
	err := db.Model(&Log{}).
		Select("COALESCE(MAX(id), 0)").
		Scan(&maxID).Error
	return maxID, err
}

func logTypesFromExpireDays(logTypeExpireDays map[int]int) []int {
	logTypes := make([]int, 0, len(logTypeExpireDays))
	for logType, days := range logTypeExpireDays {
		if days > 0 {
			logTypes = append(logTypes, logType)
		}
	}
	return logTypes
}

func rebuildExpiriesFromLogs(startTime int64, snapshotMaxLogID int, logTypeExpireDays map[int]int, logTypes []int, stats *RebuildQuotaExpiryStats, report func(string)) error {
	lastID := 0
	for {
		var logs []*Log
		err := LOG_DB.
			Select("id", "user_id", "type", "quota", "created_at").
			Where("id > ? AND id <= ? AND created_at >= ? AND type IN ? AND quota > 0", lastID, snapshotMaxLogID, startTime, logTypes).
			Order("id ASC").
			Limit(quotaExpiryRebuildLogBatchSize).
			Find(&logs).Error
		if err != nil {
			return fmt.Errorf("failed to query logs: %w", err)
		}
		if len(logs) == 0 {
			break
		}

		expiries := make([]*LogQuotaExpiry, 0, len(logs))
		for _, log := range logs {
			lastID = log.Id
			stats.ScannedLogCount++

			days, ok := logTypeExpireDays[log.Type]
			if !ok || days <= 0 {
				continue
			}
			expiries = append(expiries, &LogQuotaExpiry{
				LogId:         log.Id,
				UserId:        log.UserId,
				OriginalQuota: log.Quota,
				ConsumedQuota: 0,
				ExpireAt:      calculateQuotaExpireAt(log.CreatedAt, days),
				Status:        LogQuotaExpiryStatusPending,
				CreatedAt:     log.CreatedAt,
			})
		}

		if len(expiries) > 0 {
			if err := upsertLogQuotaExpiryBatch(expiries); err != nil {
				return fmt.Errorf("failed to upsert expiries: %w", err)
			}
			stats.RebuiltExpiryCount += len(expiries)
		}
		stats.CurrentLogID = lastID
		report("upserting_expiries")

		if len(logs) < quotaExpiryRebuildLogBatchSize {
			break
		}
	}
	return nil
}

func upsertLogQuotaExpiryBatch(expiries []*LogQuotaExpiry) error {
	return upsertLogQuotaExpiryBatchWithDB(DB, expiries)
}

func upsertLogQuotaExpiryBatchWithDB(db *gorm.DB, expiries []*LogQuotaExpiry) error {
	if len(expiries) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "log_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"user_id",
			"original_quota",
			"expire_at",
			"created_at",
		}),
	}).CreateInBatches(expiries, quotaExpiryRebuildLogBatchSize).Error
}

func cleanupStaleExpiriesFromDate(startTime int64, snapshotMaxLogID int, logTypes []int) (int, error) {
	if snapshotMaxLogID <= 0 {
		return 0, nil
	}

	deleted := 0
	lastID := 0
	for {
		var expiries []*LogQuotaExpiry
		err := DB.
			Select("id", "log_id").
			Where("id > ? AND created_at >= ? AND log_id <= ?", lastID, startTime, snapshotMaxLogID).
			Order("id ASC").
			Limit(quotaExpiryRebuildLogBatchSize).
			Find(&expiries).Error
		if err != nil {
			return deleted, fmt.Errorf("failed to query stale expiry candidates: %w", err)
		}
		if len(expiries) == 0 {
			break
		}
		lastID = expiries[len(expiries)-1].Id

		logIDs := make([]int, 0, len(expiries))
		for _, expiry := range expiries {
			logIDs = append(logIDs, expiry.LogId)
		}

		validLogIDs := map[int]struct{}{}
		if len(logTypes) > 0 {
			var ids []int
			err = LOG_DB.Model(&Log{}).
				Where("id IN ? AND id <= ? AND created_at >= ? AND type IN ? AND quota > 0", logIDs, snapshotMaxLogID, startTime, logTypes).
				Pluck("id", &ids).Error
			if err != nil {
				return deleted, fmt.Errorf("failed to query valid log ids: %w", err)
			}
			for _, id := range ids {
				validLogIDs[id] = struct{}{}
			}
		}

		staleIDs := make([]int, 0)
		for _, expiry := range expiries {
			if _, ok := validLogIDs[expiry.LogId]; !ok {
				staleIDs = append(staleIDs, expiry.Id)
			}
		}
		if len(staleIDs) > 0 {
			result := DB.Where("id IN ?", staleIDs).Delete(&LogQuotaExpiry{})
			if result.Error != nil {
				return deleted, fmt.Errorf("failed to delete stale expiries: %w", result.Error)
			}
			deleted += int(result.RowsAffected)
		}

		if len(expiries) < quotaExpiryRebuildLogBatchSize {
			break
		}
	}

	return deleted, nil
}

func rebuildAffectedUsersFromDate(startTime int64, snapshotMaxLogID int, stats *RebuildQuotaExpiryStats, report func(string)) error {
	if snapshotMaxLogID <= 0 {
		return nil
	}

	lastUserID := 0
	for {
		var userBatch []int
		err := DB.Model(&LogQuotaExpiry{}).
			Where("created_at >= ? AND log_id <= ? AND user_id > ?", startTime, snapshotMaxLogID, lastUserID).
			Distinct("user_id").
			Order("user_id ASC").
			Limit(quotaExpiryRebuildUserBatchSize).
			Pluck("user_id", &userBatch).Error
		if err != nil {
			return fmt.Errorf("failed to query affected users: %w", err)
		}
		if len(userBatch) == 0 {
			break
		}

		stats.AffectedUserCount += len(userBatch)
		if err := rebuildAffectedUserBatch(userBatch, startTime, snapshotMaxLogID, stats); err != nil {
			return err
		}
		lastUserID = userBatch[len(userBatch)-1]
		stats.CurrentUserID = lastUserID
		report("rebuilding_users")

		if len(userBatch) < quotaExpiryRebuildUserBatchSize {
			break
		}
	}

	return nil
}

func rebuildAffectedUserBatch(userIDs []int, startTime int64, snapshotMaxLogID int, stats *RebuildQuotaExpiryStats) error {
	workers := quotaExpiryRebuildWorkerCount()
	if workers > len(userIDs) {
		workers = len(userIDs)
	}
	if workers <= 0 {
		workers = 1
	}

	userCh := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	failed := 0
	updatedConsumed := 0

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for userID := range userCh {
				updated, err := rebuildUserConsumedQuota(userID, startTime, snapshotMaxLogID)
				mu.Lock()
				if err != nil {
					failed++
					common.SysLog(fmt.Sprintf("failed to rebuild consumed_quota for user %d: %v", userID, err))
				} else {
					updatedConsumed += updated
				}
				mu.Unlock()
			}
		}()
	}

	for _, userID := range userIDs {
		userCh <- userID
	}
	close(userCh)
	wg.Wait()

	if err := rebuildUsersUsedQuota(userIDs); err != nil {
		return fmt.Errorf("failed to rebuild used_quota: %w", err)
	}

	stats.ProcessedUserCount += len(userIDs) - failed
	stats.FailedUserCount += failed
	stats.UpdatedConsumedCount += updatedConsumed
	return nil
}

func quotaExpiryRebuildWorkerCount() int {
	if common.UsingSQLite || (LOG_DB != DB && common.LogSqlType == common.DatabaseTypeSQLite) {
		return 1
	}
	workers := common.GetEnvOrDefault("QUOTA_EXPIRY_REBUILD_WORKERS", 4)
	if workers < 1 {
		return 1
	}
	if workers > 16 {
		return 16
	}
	return workers
}

// rebuildUserConsumedQuota 重新计算指定用户从 startTime 开始的消费归因。
func rebuildUserConsumedQuota(userId int, startTime int64, snapshotMaxLogID int) (int, error) {
	var consumeLogs []*Log
	err := LOG_DB.
		Select("id", "quota", "created_at").
		Where("id <= ? AND user_id = ? AND type IN ? AND created_at >= ? AND quota > 0", snapshotMaxLogID, userId, quotaConsumeLogTypes, startTime).
		Order("created_at ASC, id ASC").
		Find(&consumeLogs).Error
	if err != nil {
		return 0, err
	}

	updated := 0
	err = DB.Transaction(func(tx *gorm.DB) error {
		var expiries []*LogQuotaExpiry
		err := tx.
			Where("user_id = ? AND created_at >= ? AND log_id <= ?", userId, startTime, snapshotMaxLogID).
			Order("created_at ASC, id ASC").
			Find(&expiries).Error
		if err != nil {
			return err
		}
		if len(expiries) == 0 {
			return nil
		}

		oldConsumed := make(map[int]int, len(expiries))
		for _, expiry := range expiries {
			oldConsumed[expiry.Id] = expiry.ConsumedQuota
			expiry.ConsumedQuota = 0
		}

		active := make([]*LogQuotaExpiry, 0, len(expiries))
		activeHead := 0
		expiryIdx := 0
		for _, log := range consumeLogs {
			for expiryIdx < len(expiries) && expiries[expiryIdx].CreatedAt <= log.CreatedAt {
				active = append(active, expiries[expiryIdx])
				expiryIdx++
			}

			remaining := log.Quota
			consumeDayStart := quotaExpiryDayStart(log.CreatedAt)
			for remaining > 0 && activeHead < len(active) {
				expiry := active[activeHead]
				if consumeDayStart >= expiry.ExpireAt || expiry.OriginalQuota <= expiry.ConsumedQuota {
					activeHead++
					continue
				}

				available := expiry.OriginalQuota - expiry.ConsumedQuota
				toConsume := remaining
				if toConsume > available {
					toConsume = available
				}
				expiry.ConsumedQuota += toConsume
				remaining -= toConsume
			}
		}

		for _, expiry := range expiries {
			if oldConsumed[expiry.Id] == expiry.ConsumedQuota {
				continue
			}
			err := tx.Model(&LogQuotaExpiry{}).
				Where("id = ?", expiry.Id).
				Update("consumed_quota", expiry.ConsumedQuota).Error
			if err != nil {
				return err
			}
			updated++
		}

		return nil
	})
	return updated, err
}

func rebuildUsersUsedQuota(userIDs []int) error {
	if len(userIDs) == 0 {
		return nil
	}

	type userQuotaTotal struct {
		UserID int   `gorm:"column:user_id"`
		Total  int64 `gorm:"column:total"`
	}
	var totals []userQuotaTotal
	err := LOG_DB.Model(&Log{}).
		Select("user_id, COALESCE(SUM(quota), 0) AS total").
		Where("user_id IN ? AND type IN ?", userIDs, quotaConsumeLogTypes).
		Group("user_id").
		Scan(&totals).Error
	if err != nil {
		return err
	}

	totalByUserID := make(map[int]int, len(totals))
	for _, total := range totals {
		totalByUserID[total.UserID] = int(total.Total)
	}

	return DB.Transaction(func(tx *gorm.DB) error {
		for _, userID := range userIDs {
			err := tx.Model(&User{}).
				Where("id = ?", userID).
				Update("used_quota", totalByUserID[userID]).Error
			if err != nil {
				return err
			}
		}
		return nil
	})
}
