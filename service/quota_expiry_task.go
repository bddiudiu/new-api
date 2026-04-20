package service

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"

	"github.com/bytedance/gopkg/util/gopool"
)

const (
	quotaExpiryBatchSize = 500
	quotaExpiryRedisKey  = "quota_expiry_ran:"
)

var (
	quotaExpiryOnce    sync.Once
	quotaExpiryRunning atomic.Bool
)

func StartQuotaExpiryTask() {
	quotaExpiryOnce.Do(func() {
		if !common.IsMasterNode {
			return
		}
		gopool.Go(func() {
			logger.LogInfo(context.Background(), "quota expiry task started")

			// 启动时检查今日是否已执行
			if !hasRunToday() {
				logger.LogInfo(context.Background(), "quota expiry task: running catch-up for today")
				runQuotaExpiryOnce()
				markRunToday()
			}

			scheduleNextMidnight()
		})
	})
}

func scheduleNextMidnight() {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	duration := time.Until(next)

	logger.LogInfo(context.Background(), fmt.Sprintf("quota expiry task: next run scheduled at %s (in %s)", next.Format("2006-01-02 15:04:05"), duration))

	time.AfterFunc(duration, func() {
		runQuotaExpiryOnce()
		markRunToday()
		scheduleNextMidnight()
	})
}

func hasRunToday() bool {
	if common.RedisEnabled {
		today := time.Now().Format("2006-01-02")
		key := quotaExpiryRedisKey + today
		val, err := common.RedisGet(key)
		return err == nil && val == "1"
	}
	return false
}

func markRunToday() {
	if common.RedisEnabled {
		today := time.Now().Format("2006-01-02")
		key := quotaExpiryRedisKey + today
		// TTL 25小时，避免跨天残留
		_ = common.RedisSet(key, "1", 25*time.Hour)
	}
}

func runQuotaExpiryOnce() {
	if !quotaExpiryRunning.CompareAndSwap(false, true) {
		logger.LogWarn(context.Background(), "quota expiry task already running, skipping")
		return
	}
	defer quotaExpiryRunning.Store(false)

	ctx := context.Background()
	totalProcessed := 0
	totalVoided := 0

	logger.LogInfo(ctx, "quota expiry task: starting batch processing")

	for {
		batch, err := model.GetPendingExpiredBatch(quotaExpiryBatchSize)
		if err != nil {
			logger.LogError(ctx, fmt.Sprintf("quota expiry task: failed to get batch: %v", err))
			return
		}

		if len(batch) == 0 {
			break
		}

		for _, expiry := range batch {
			voidQuota := expiry.OriginalQuota - expiry.ConsumedQuota

			if voidQuota > 0 {
				// 扣除用户额度
				err = model.DecreaseUserQuota(expiry.UserId, voidQuota, false)
				if err != nil {
					logger.LogError(ctx, fmt.Sprintf("quota expiry task: failed to decrease quota for expiry %d: %v", expiry.Id, err))
					continue
				}

				// 写入作废日志
				content := fmt.Sprintf("有效期到期-%d额度作废", voidQuota)
				model.RecordLog(expiry.UserId, model.LogTypeConsume, content)

				totalVoided += voidQuota
			}

			// 标记为已处理
			err = model.MarkProcessed(expiry.Id)
			if err != nil {
				logger.LogError(ctx, fmt.Sprintf("quota expiry task: failed to mark processed for expiry %d: %v", expiry.Id, err))
			}

			totalProcessed++
		}

		if len(batch) < quotaExpiryBatchSize {
			break
		}
	}

	logger.LogInfo(ctx, fmt.Sprintf("quota expiry task completed: processed=%d, voided=%d", totalProcessed, totalVoided))
}
