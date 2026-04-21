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
		scheduleNextMidnight()
	})
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
			voidQuota, processed, err := model.ProcessQuotaExpiry(expiry.Id)
			if err != nil {
				logger.LogError(ctx, fmt.Sprintf("quota expiry task: failed to process expiry %d: %v", expiry.Id, err))
				continue
			}
			if !processed {
				continue
			}
			if voidQuota > 0 {
				model.RecordQuotaExpiryVoidLog(expiry.UserId, voidQuota)
				totalVoided += voidQuota
			}
			totalProcessed++
		}

		if len(batch) < quotaExpiryBatchSize {
			break
		}
	}

	logger.LogInfo(ctx, fmt.Sprintf("quota expiry task completed: processed=%d, voided=%d", totalProcessed, totalVoided))
}
