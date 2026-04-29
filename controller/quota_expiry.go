package controller

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/operation_setting"

	"github.com/bytedance/gopkg/util/gopool"
	"github.com/gin-gonic/gin"
)

type RebuildQuotaExpiryRequest struct {
	StartDate string `json:"start_date" binding:"required"` // 格式：2026-01-01
}

type RebuildQuotaExpiryResponse struct {
	TaskId string                         `json:"task_id"`
	Status string                         `json:"status"` // running / completed / failed
	Error  string                         `json:"error,omitempty"`
	Stats  *model.RebuildQuotaExpiryStats `json:"stats,omitempty"`
}

var (
	rebuildTaskStatus = make(map[string]*RebuildQuotaExpiryResponse)
	rebuildTaskMutex  sync.RWMutex
)

// RebuildQuotaExpiry 重建额度有效期数据
func RebuildQuotaExpiry(c *gin.Context) {
	var req RebuildQuotaExpiryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": err.Error()})
		return
	}

	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		location = time.FixedZone("CST", 8*3600)
	}

	// 解析日期
	startTime, err := time.ParseInLocation("2006-01-02", req.StartDate, location)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": "日期格式错误，应为 YYYY-MM-DD"})
		return
	}

	// 生成任务 ID
	taskId := fmt.Sprintf("rebuild_%d", time.Now().UnixNano())

	// 初始化任务状态
	rebuildTaskMutex.Lock()
	rebuildTaskStatus[taskId] = &RebuildQuotaExpiryResponse{
		TaskId: taskId,
		Status: "running",
		Stats:  &model.RebuildQuotaExpiryStats{Phase: "queued"},
	}
	rebuildTaskMutex.Unlock()

	// 异步执行重建
	gopool.Go(func() {
		logTypeExpireDays := operation_setting.GetLogTypeExpireDaysMap()
		stats, err := model.RebuildExpiriesFromDateWithProgressAndJobID(
			startTime.Unix(),
			logTypeExpireDays,
			taskId,
			func(stats model.RebuildQuotaExpiryStats) {
				rebuildTaskMutex.Lock()
				defer rebuildTaskMutex.Unlock()
				if status, ok := rebuildTaskStatus[taskId]; ok {
					status.Stats = &stats
				}
			},
		)

		rebuildTaskMutex.Lock()
		defer rebuildTaskMutex.Unlock()
		if err != nil {
			rebuildTaskStatus[taskId].Status = "failed"
			rebuildTaskStatus[taskId].Error = err.Error()
		} else {
			rebuildTaskStatus[taskId].Status = "completed"
			rebuildTaskStatus[taskId].Stats = stats
		}
	})

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "重建任务已启动",
		"data": func() *RebuildQuotaExpiryResponse {
			rebuildTaskMutex.RLock()
			defer rebuildTaskMutex.RUnlock()
			return rebuildTaskStatus[taskId]
		}(),
	})
}

// GetRebuildStatus 查询重建任务状态
func GetRebuildStatus(c *gin.Context) {
	taskId := c.Query("task_id")
	if taskId == "" {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "message": "缺少 task_id 参数"})
		return
	}

	rebuildTaskMutex.RLock()
	status, ok := rebuildTaskStatus[taskId]
	rebuildTaskMutex.RUnlock()
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "message": "任务不存在"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    status,
	})
}
