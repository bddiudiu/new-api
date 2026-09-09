package model

import (
	"fmt"
	"strings"
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
	VoidQuota     int    `gorm:"not null;default:0"`
	ProcessedAt   int64  `gorm:"not null;default:0"`
	VoidLogID     int    `gorm:"not null;default:0"`
	CachePending  bool   `gorm:"not null;default:false"`
}

// QuotaExpiryLogOutbox lives in the log database. It commits with the source
// log, so a main-database failure cannot lose the accounting event.
type QuotaExpiryLogOutbox struct {
	LogId        int `gorm:"primaryKey;autoIncrement:false"`
	UserId       int
	LogType      int
	Quota        int
	LogCreatedAt int64
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
	QuotaExpiryRuntimeModeFailed     = "failed"

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

	if common.UsingMainDatabase(common.DatabaseTypeSQLite) {
		err := tx.Model(&QuotaExpiryRuntimeState{}).
			Where("id = ?", quotaExpiryRuntimeStateID).
			UpdateColumn("updated_at", gorm.Expr("updated_at")).Error
		if err != nil {
			return nil, err
		}
	}

	var state QuotaExpiryRuntimeState
	query := lockForUpdate(tx).Where("id = ?", quotaExpiryRuntimeStateID)
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
	return state.Mode != QuotaExpiryRuntimeModeNormal, nil
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

		return handleQuotaExpiryLogWithState(tx, state, log)
	})
}

// Serialize log creation with rebuild snapshots, so attribution cannot arrive
// before its grant or be counted both by the snapshot and a delayed callback.
func createLogWithQuotaExpiry(log *Log) error {
	if !isQuotaExpiryTrackedLog(log) {
		return createLog(log)
	}
	// ClickHouse has no transactional outbox or stable relational log ID.
	// Preserve its existing append-and-attribute path for consumption logs.
	if common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
		if err := createLog(log); err != nil {
			return err
		}
		return HandleQuotaExpiryLog(log)
	}
	var delivered []int
	err := DB.Transaction(func(tx *gorm.DB) error {
		state, err := lockQuotaExpiryRuntimeState(tx)
		if err != nil {
			return err
		}
		ensureLogRequestId(log)
		if LOG_DB == DB {
			if err := tx.Create(log).Error; err != nil {
				return err
			}
			return handleQuotaExpiryLogWithState(tx, state, log)
		}
		if err := LOG_DB.Transaction(func(logTx *gorm.DB) error {
			if err := logTx.Create(log).Error; err != nil {
				return err
			}
			return logTx.Create(&QuotaExpiryLogOutbox{
				LogId: log.Id, UserId: log.UserId, LogType: log.Type,
				Quota: log.Quota, LogCreatedAt: log.CreatedAt,
			}).Error
		}); err != nil {
			return err
		}
		// Older failed events must retain their FIFO share before this event.
		delivered, err = deliverQuotaExpiryLogOutbox(tx, state)
		return err
	})
	if err != nil {
		return err
	}
	return acknowledgeQuotaExpiryLogOutbox(delivered)
}

func handleQuotaExpiryLogWithState(tx *gorm.DB, state *QuotaExpiryRuntimeState, log *Log) error {
	if log.Id > 0 && log.Id <= state.SnapshotMaxLogID {
		return nil
	}
	if state.Mode != QuotaExpiryRuntimeModeNormal {
		if log.Id <= 0 {
			return fmt.Errorf("quota expiry accounting requires a persisted log during rebuild")
		}
		return enqueueQuotaExpiryReplayLog(tx, state.JobId, log)
	}
	if log.Id <= 0 {
		return applyQuotaExpiryLogWithDB(tx, log)
	}
	if err := enqueueQuotaExpiryReplayLog(tx, "", log); err != nil {
		return err
	}
	var receipt QuotaExpiryReplayLog
	if err := tx.Where("log_id = ?", log.Id).First(&receipt).Error; err != nil {
		return err
	}
	if receipt.Status == QuotaExpiryReplayStatusProcessed {
		return nil
	}
	if err := applyQuotaExpiryLogWithDB(tx, log); err != nil {
		return err
	}
	return tx.Model(&receipt).Update("status", QuotaExpiryReplayStatusProcessed).Error
}

// When detailed consumption logging is disabled, keep the accounting event
// needed to rebuild expiring grants, without request or response details.
func recordQuotaExpiryConsumption(log *Log) {
	if !isQuotaExpiryTrackedLog(log) {
		return
	}
	if len(operation_setting.GetLogTypeExpireDaysMap()) == 0 {
		var pending int64
		err := DB.Model(&LogQuotaExpiry{}).Where("user_id = ? AND status = ?", log.UserId, LogQuotaExpiryStatusPending).Limit(1).Count(&pending).Error
		if err != nil {
			common.SysError("failed to check quota expiry accounting: " + err.Error())
			return
		}
		if pending == 0 {
			return
		}
	}
	// Subscription events were excluded above; no other metadata is needed
	// for wallet accounting when detailed logs are disabled.
	log.Other = ""
	if err := createLogWithQuotaExpiry(log); err != nil {
		common.SysError("failed to record quota expiry consumption: " + err.Error())
	}
}

func ValidateQuotaExpiryRulesJSON(value string) error {
	var rules []operation_setting.LogTypeExpiryRule
	if err := common.UnmarshalJsonStr(value, &rules); err != nil {
		return fmt.Errorf("invalid quota expiry rules: %w", err)
	}
	if rules == nil {
		return fmt.Errorf("quota expiry rules must be an array")
	}
	seen := make(map[int]bool, len(rules))
	for _, rule := range rules {
		if strings.TrimSpace(rule.Label) == "" || rule.ExpireDays <= 0 || rule.ExpireDays > operation_setting.MaxQuotaExpiryDays {
			return fmt.Errorf("quota expiry rules require a label and an expiry of 1 to 36500 days")
		}
		switch rule.LogType {
		case LogTypeSystem, LogTypeActive, LogTypeCheckin:
		default:
			return fmt.Errorf("log type %d is not a quota grant", rule.LogType)
		}
		if seen[rule.LogType] {
			return fmt.Errorf("duplicate quota expiry log type %d", rule.LogType)
		}
		seen[rule.LogType] = true
	}
	return nil
}

func isQuotaExpiryTrackedLog(log *Log) bool {
	if log == nil || log.UserId <= 0 || log.Quota <= 0 {
		return false
	}
	if IsQuotaConsumeLogType(log.Type) {
		var other struct {
			BillingSource string `json:"billing_source"`
		}
		if log.Other != "" {
			_ = common.UnmarshalJsonStr(log.Other, &other)
		}
		return other.BillingSource != "subscription"
	}
	switch log.Type {
	case LogTypeSystem, LogTypeActive, LogTypeCheckin:
		return operation_setting.GetExpireDaysForLogType(log.Type) > 0
	default:
		return false
	}
}

func applyQuotaExpiryLogWithDB(db *gorm.DB, log *Log) error {
	if !isQuotaExpiryTrackedLog(log) {
		return nil
	}
	if log.CreatedAt <= 0 {
		log.CreatedAt = common.GetTimestamp()
	}
	if IsQuotaConsumeLogType(log.Type) {
		return applyConsumeToExpiriesWithDB(db, log.UserId, log.Quota, log.CreatedAt, log.Id)
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
	return HandleQuotaExpiryLog(&Log{UserId: userId, Type: LogTypeConsume, Quota: consumeQuota, CreatedAt: consumeAt})
}

func applyConsumeToExpiriesWithDB(db *gorm.DB, userId int, consumeQuota int, consumeAt int64, consumeLogID int) error {
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
		query := db.Where(
			"user_id = ? AND status = ? AND created_at <= ? AND expire_at > ? AND original_quota > consumed_quota",
			userId,
			LogQuotaExpiryStatusPending,
			consumeAt,
			consumeDayStart,
		)
		if consumeLogID > 0 {
			query = query.Where("created_at < ? OR (created_at = ? AND log_id < ?)", consumeAt, consumeAt, consumeLogID)
		}
		err := query.Order("created_at ASC, log_id ASC").First(&batch).Error
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
	var delivered []int

	err := DB.Transaction(func(tx *gorm.DB) error {
		state, err := lockQuotaExpiryRuntimeState(tx)
		if err != nil {
			return err
		}
		if state.Mode != QuotaExpiryRuntimeModeNormal {
			return nil
		}

		if LOG_DB != DB && !common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
			delivered, err = deliverQuotaExpiryLogOutbox(tx, state)
			if err != nil {
				return err
			}
		}

		result := tx.Model(&LogQuotaExpiry{}).
			Where("id = ? AND status = ? AND expire_at <= ?", id, LogQuotaExpiryStatusPending, quotaExpiryDayStart(common.GetTimestamp())).
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
		voidQuota = max(0, expiry.OriginalQuota-expiry.ConsumedQuota)
		if voidQuota > 0 {
			var user User
			if err := lockForUpdate(tx).First(&user, expiry.UserId).Error; err != nil {
				return err
			}
			voidQuota = max(0, min(voidQuota, user.Quota))
			if err := tx.Model(&User{}).Where("id = ?", user.Id).
				Update("quota", gorm.Expr("quota - ?", voidQuota)).Error; err != nil {
				return err
			}
		}
		expiry.VoidQuota = max(0, voidQuota)
		expiry.CachePending = common.RedisEnabled && expiry.VoidQuota > 0
		expiry.ProcessedAt = common.GetTimestamp()
		expiry.Status = LogQuotaExpiryStatusProcessed
		if LOG_DB == DB && expiry.VoidQuota > 0 {
			log, err := quotaExpiryVoidLog(tx, &expiry)
			if err != nil {
				return err
			}
			if err := tx.Create(log).Error; err != nil {
				return err
			}
			expiry.VoidLogID = log.Id
		}
		return tx.Save(&expiry).Error
	})
	if err != nil {
		return 0, false, err
	}
	if err := acknowledgeQuotaExpiryLogOutbox(delivered); err != nil {
		common.SysError("failed to acknowledge expiry accounting: " + err.Error())
	}
	if !claimed {
		return 0, false, nil
	}
	if err := syncQuotaExpiryCache(id, true); err != nil {
		return voidQuota, true, err
	}
	if err := FlushQuotaExpiryVoidLogs(); err != nil {
		return voidQuota, true, err
	}
	common.SysLog(fmt.Sprintf("quota expiry processed: reason=%s expiry_id=%d user_id=%d expire_date=%s void_quota=%d", reason, id, userId, quotaExpiryDateOnly(expireAt), voidQuota))
	return voidQuota, true, nil
}

func processExpiredExpiriesFromDate(startTime int64) (*RebuildQuotaExpiryStats, error) {
	stats := &RebuildQuotaExpiryStats{}
	if err := FlushQuotaExpiryVoidLogs(); err != nil {
		return nil, err
	}
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

		processedInBatch := 0
		for _, expiry := range pendingExpiries {
			voidQuota, processed, err := ProcessQuotaExpiryWithReason(expiry.Id, "rebuild_init")
			if err != nil {
				return nil, err
			}
			if !processed {
				continue
			}
			stats.ProcessedExpiredCount++
			processedInBatch++
			stats.ProcessedExpiredVoidQuota += int64(voidQuota)
			if voidQuota <= 0 {
				continue
			}
			stats.GeneratedExpiryLogCount++
		}

		if len(pendingExpiries) < quotaExpiryRebuildUpdateBatchSize || processedInBatch == 0 {
			break
		}
	}
	return stats, nil
}

// RebuildExpiriesFromDate 从指定日期开始按快照重建 expiry 记录。
func RebuildExpiriesFromDate(startTime int64, logTypeExpireDays map[int]int) (*RebuildQuotaExpiryStats, error) {
	return RebuildExpiriesFromDateWithProgress(startTime, logTypeExpireDays, nil)
}

func RebuildExpiriesFromDateWithProgress(startTime int64, logTypeExpireDays map[int]int, progress RebuildQuotaExpiryProgress) (*RebuildQuotaExpiryStats, error) {
	jobID := fmt.Sprintf("rebuild_%d", time.Now().UnixNano())
	return RebuildExpiriesFromDateWithProgressAndJobID(startTime, logTypeExpireDays, jobID, progress)
}

func RebuildExpiriesFromDateWithProgressAndJobID(startTime int64, logTypeExpireDays map[int]int, jobID string, progress RebuildQuotaExpiryProgress) (_ *RebuildQuotaExpiryStats, rebuildErr error) {
	for logType, days := range logTypeExpireDays {
		if days <= 0 || days > operation_setting.MaxQuotaExpiryDays {
			return nil, fmt.Errorf("invalid expiry days for log type %d", logType)
		}
		switch logType {
		case LogTypeSystem, LogTypeActive, LogTypeCheckin:
		default:
			return nil, fmt.Errorf("log type %d is not a quota grant", logType)
		}
	}
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
	// Only an incomplete attribution rebuild must pause expiration. After
	// finalization, expiration failures can be retried by the normal scheduler.
	defer func() {
		if rebuildErr != nil {
			if err := DB.Model(&QuotaExpiryRuntimeState{}).
				Where("id = ? AND job_id = ? AND mode = ?", quotaExpiryRuntimeStateID, jobID, QuotaExpiryRuntimeModeRebuilding).
				Update("mode", QuotaExpiryRuntimeModeFailed).Error; err != nil {
				common.SysError("failed to mark quota expiry rebuild failure: " + err.Error())
			}
		}
	}()
	stats.SnapshotMaxLogID = snapshotMaxLogID
	report("snapshot")

	logTypes := logTypesFromExpireDays(logTypeExpireDays)
	if len(logTypes) > 0 {
		if err := rebuildExpiriesFromLogs(jobID, startTime, snapshotMaxLogID, logTypeExpireDays, logTypes, stats, report); err != nil {
			return nil, err
		}
	}

	report("cleaning_stale")
	deleted, err := cleanupStaleExpiriesFromDate(jobID, startTime, snapshotMaxLogID, logTypes)
	if err != nil {
		return nil, err
	}
	stats.DeletedStaleCount = deleted
	report("rebuilding_users")

	if err := rebuildAffectedUsersFromDate(jobID, startTime, snapshotMaxLogID, stats, report); err != nil {
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
		if state.Mode == QuotaExpiryRuntimeModeRebuilding && common.GetTimestamp()-state.UpdatedAt < quotaExpiryRebuildLeaseSeconds {
			return fmt.Errorf("quota expiry rebuild already running: job_id=%s snapshot_max_log_id=%d", state.JobId, state.SnapshotMaxLogID)
		}

		if state.Mode != QuotaExpiryRuntimeModeNormal && startTime > state.StartTime {
			return fmt.Errorf("retry the failed rebuild from its original start date or earlier")
		}

		snapshotDB := LOG_DB
		if LOG_DB == DB {
			snapshotDB = tx
		}
		snapshot, err := getQuotaExpiryRebuildSnapshotMaxLogIDWithDB(snapshotDB)
		if err != nil {
			return fmt.Errorf("failed to query rebuild snapshot: %w", err)
		}

		if LOG_DB != DB && !common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
			if _, err := deliverQuotaExpiryLogOutbox(tx, state); err != nil {
				return err
			}
		}

		// Snapshot events are covered by reconstruction. Keep their receipts:
		// a split-database acknowledgement may still be retried after this
		// rebuild, even if log retention later lowers the snapshot maximum.
		if err := tx.Model(&QuotaExpiryReplayLog{}).Where("log_id <= ?", snapshot).
			Update("status", QuotaExpiryReplayStatusProcessed).Error; err != nil {
			return err
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
		err := quotaExpiryRebuildTransaction(jobID, func(tx *gorm.DB) error {
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

func rebuildExpiriesFromLogs(jobID string, startTime int64, snapshotMaxLogID int, logTypeExpireDays map[int]int, logTypes []int, stats *RebuildQuotaExpiryStats, report func(string)) error {
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
			if err := quotaExpiryRebuildTransaction(jobID, func(tx *gorm.DB) error { return upsertLogQuotaExpiryBatchWithDB(tx, expiries) }); err != nil {
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

func upsertLogQuotaExpiryBatchWithDB(db *gorm.DB, expiries []*LogQuotaExpiry) error {
	if len(expiries) == 0 {
		return nil
	}
	logIDs := make([]int, 0, len(expiries))
	for _, expiry := range expiries {
		logIDs = append(logIDs, expiry.LogId)
	}
	var processedIDs []int
	if err := db.Model(&LogQuotaExpiry{}).Where("log_id IN ? AND status <> ?", logIDs, LogQuotaExpiryStatusPending).
		Pluck("log_id", &processedIDs).Error; err != nil {
		return err
	}
	processed := make(map[int]bool, len(processedIDs))
	for _, id := range processedIDs {
		processed[id] = true
	}
	pending := make([]*LogQuotaExpiry, 0, len(expiries))
	for _, expiry := range expiries {
		if !processed[expiry.LogId] {
			pending = append(pending, expiry)
		}
	}
	if len(pending) == 0 {
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
	}).CreateInBatches(pending, quotaExpiryRebuildLogBatchSize).Error
}

func cleanupStaleExpiriesFromDate(jobID string, startTime int64, snapshotMaxLogID int, logTypes []int) (int, error) {
	deleted := 0
	err := quotaExpiryRebuildTransaction(jobID, func(tx *gorm.DB) error {
		logDB := LOG_DB
		if LOG_DB == DB {
			logDB = tx
		}
		lastID := 0
		for {
			var expiries []*LogQuotaExpiry
			err := tx.
				Select("id", "log_id").
				Where("id > ? AND created_at >= ? AND status = ?", lastID, startTime, LogQuotaExpiryStatusPending).
				Order("id ASC").
				Limit(quotaExpiryRebuildLogBatchSize).
				Find(&expiries).Error
			if err != nil {
				return fmt.Errorf("failed to query stale expiry candidates: %w", err)
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
				err = logDB.Model(&Log{}).
					Where("id IN ? AND id <= ? AND created_at >= ? AND type IN ? AND quota > 0", logIDs, snapshotMaxLogID, startTime, logTypes).
					Pluck("id", &ids).Error
				if err != nil {
					return fmt.Errorf("failed to query valid log ids: %w", err)
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
				result := tx.Where("id IN ?", staleIDs).Delete(&LogQuotaExpiry{})
				if result.Error != nil {
					return fmt.Errorf("failed to delete stale expiries: %w", result.Error)
				}
				deleted += int(result.RowsAffected)
			}

			if len(expiries) < quotaExpiryRebuildLogBatchSize {
				break
			}
		}

		return nil
	})
	return deleted, err
}

func rebuildAffectedUsersFromDate(jobID string, startTime int64, snapshotMaxLogID int, stats *RebuildQuotaExpiryStats, report func(string)) error {
	if snapshotMaxLogID <= 0 {
		return nil
	}

	lastUserID := 0
	for {
		var userBatch []int
		err := DB.Model(&LogQuotaExpiry{}).
			Where("(created_at >= ? OR status = ?) AND log_id <= ? AND user_id > ?", startTime, LogQuotaExpiryStatusPending, snapshotMaxLogID, lastUserID).
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
		if err := rebuildAffectedUserBatch(jobID, userBatch, snapshotMaxLogID, stats); err != nil {
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

func rebuildAffectedUserBatch(jobID string, userIDs []int, snapshotMaxLogID int, stats *RebuildQuotaExpiryStats) error {
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
				updated, err := rebuildUserConsumedQuota(jobID, userID, snapshotMaxLogID)
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

	stats.ProcessedUserCount += len(userIDs) - failed
	stats.FailedUserCount += failed
	stats.UpdatedConsumedCount += updatedConsumed
	if failed > 0 {
		return fmt.Errorf("failed to rebuild quota consumption for %d users", failed)
	}
	return nil
}

func quotaExpiryRebuildWorkerCount() int {
	if common.UsingMainDatabase(common.DatabaseTypeSQLite) || (LOG_DB != DB && common.UsingLogDatabase(common.DatabaseTypeSQLite)) {
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

// rebuildUserConsumedQuota replays existing batches as well as newly rebuilt
// grants, so older pending grants retain their FIFO share during retries.
func rebuildUserConsumedQuota(jobID string, userId int, snapshotMaxLogID int) (int, error) {
	var earliestCreatedAt int64
	if err := DB.Model(&LogQuotaExpiry{}).Where("user_id = ? AND log_id <= ?", userId, snapshotMaxLogID).
		Select("COALESCE(MIN(created_at), 0)").Scan(&earliestCreatedAt).Error; err != nil {
		return 0, err
	}
	var consumeLogs []*Log
	err := LOG_DB.
		Select("id", "user_id", "type", "quota", "created_at", "other").
		Where("id <= ? AND user_id = ? AND type IN ? AND created_at >= ? AND quota > 0", snapshotMaxLogID, userId, quotaConsumeLogTypes, earliestCreatedAt).
		Order("created_at ASC, id ASC").
		Find(&consumeLogs).Error
	if err != nil {
		return 0, err
	}

	updated := 0
	err = quotaExpiryRebuildTransaction(jobID, func(tx *gorm.DB) error {
		var expiries []*LogQuotaExpiry
		err := tx.
			Where("user_id = ? AND log_id <= ?", userId, snapshotMaxLogID).
			Order("created_at ASC, log_id ASC").
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
			if !isQuotaExpiryTrackedLog(log) {
				continue
			}
			for expiryIdx < len(expiries) && (expiries[expiryIdx].CreatedAt < log.CreatedAt || (expiries[expiryIdx].CreatedAt == log.CreatedAt && expiries[expiryIdx].LogId < log.Id)) {
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
			// Processed batches still absorb their historical FIFO consumption,
			// but their settled accounting must never be rewritten.
			// Log retention is not a refund. A partial history must never undo
			// consumption already confirmed against a grant.
			expiry.ConsumedQuota = max(oldConsumed[expiry.Id], expiry.ConsumedQuota)
			if expiry.Status != LogQuotaExpiryStatusPending || oldConsumed[expiry.Id] == expiry.ConsumedQuota {
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
