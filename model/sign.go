package model

import (
	"errors"
	"fmt"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
)

// SignResult 签到结果
type SignResult struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Quota   int    `json:"quota"` // 获得的额度
}

// SignStatus 单日签到状态
type SignStatus struct {
	Date   string `json:"date"`   // 日期 YYYY-MM-DD
	Signed bool   `json:"signed"` // 是否已签到
}

// GetTodayStartTimestamp 获取今天开始的时间戳（0点）
func GetTodayStartTimestamp() int64 {
	now := time.Now()
	year, month, day := now.Date()
	loc := now.Location()
	return time.Date(year, month, day, 0, 0, 0, 0, loc).Unix()
}

// GetTodayEndTimestamp 获取今天结束的时间戳（23:59:59）
func GetTodayEndTimestamp() int64 {
	now := time.Now()
	year, month, day := now.Date()
	loc := now.Location()
	return time.Date(year, month, day, 23, 59, 59, 999999999, loc).Unix()
}

// CheckUserSignEligibility 检查用户签到资格
// 返回：是否有资格签到，错误信息
func CheckUserSignEligibility(userId int) (bool, string) {
	// 1. 检查签到功能是否启用（QuotaForSign > 0）
	if common.QuotaForSign <= 0 {
		return false, "签到功能未启用"
	}

	// 2. 获取用户信息
	user, err := GetUserById(userId, false)
	if err != nil {
		return false, "获取用户信息失败"
	}

	// 3. 检查用户所属分组是否允许签到
	if !ratio_setting.IsGroupSignEnabled(user.Group) {
		return false, "您所在的用户组不允许签到"
	}

	// 4. 检查用户注册时间是否在允许签到的天数内
	// 老用户（没有注册时间记录）不能签到
	if user.CreatedTime <= 0 {
		return false, "签到功能仅限新注册用户"
	}
	registeredDays := (common.GetTimestamp() - user.CreatedTime) / (24 * 60 * 60)
	if int(registeredDays) >= common.SignInDays {
		return false, fmt.Sprintf("签到仅限注册后%d天内的用户", common.SignInDays)
	}

	// 5. 检查今天是否已签到
	hasSignedToday, err := HasUserSignedToday(userId)
	if err != nil {
		return false, "检查签到状态失败"
	}
	if hasSignedToday {
		return false, "今天已经签到过了"
	}

	return true, ""
}

// HasUserSignedToday 检查用户今天是否已签到
func HasUserSignedToday(userId int) (bool, error) {
	todayStart := GetTodayStartTimestamp()
	todayEnd := GetTodayEndTimestamp()

	var count int64
	err := LOG_DB.Model(&Log{}).Where(
		"user_id = ? AND type = ? AND created_at >= ? AND created_at <= ?",
		userId, LogTypeSign, todayStart, todayEnd,
	).Count(&count).Error

	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// DoSign 执行签到
func DoSign(userId int) (*SignResult, error) {
	// 1. 检查签到资格
	eligible, reason := CheckUserSignEligibility(userId)
	if !eligible {
		return &SignResult{
			Success: false,
			Message: reason,
			Quota:   0,
		}, nil
	}

	// 2. 获取用户信息用于记录
	username, _ := GetUsernameById(userId, false)

	// 3. 增加用户额度
	quota := common.QuotaForSign
	err := IncreaseUserQuota(userId, quota, true)
	if err != nil {
		return nil, errors.New("增加额度失败：" + err.Error())
	}

	// 4. 记录签到日志
	log := &Log{
		UserId:    userId,
		Username:  username,
		CreatedAt: common.GetTimestamp(),
		Type:      LogTypeSign,
		Content:   fmt.Sprintf("签到获得 %s", logger.LogQuota(quota)),
		Quota:     quota,
	}
	err = LOG_DB.Create(log).Error
	if err != nil {
		common.SysLog("failed to record sign log: " + err.Error())
	}

	return &SignResult{
		Success: true,
		Message: fmt.Sprintf("签到成功，获得 %s", logger.LogQuota(quota)),
		Quota:   quota,
	}, nil
}

// GetUserSignList 获取用户签到列表
// 返回用户注册后到今天为止每天的签到状态
func GetUserSignList(userId int) ([]SignStatus, error) {
	// 获取用户信息
	user, err := GetUserById(userId, false)
	if err != nil {
		return nil, err
	}

	// 确定起始时间
	var startTime time.Time
	if user.CreatedTime > 0 {
		startTime = time.Unix(user.CreatedTime, 0)
	} else {
		// 如果没有注册时间，使用30天前作为起始时间
		startTime = time.Now().AddDate(0, 0, -30)
	}

	// 结束时间为今天
	now := time.Now()
	endTime := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, now.Location())

	// 计算允许签到的截止日期
	maxSignDays := common.SignInDays
	signDeadline := startTime.AddDate(0, 0, maxSignDays)

	// 如果允许签到的截止日期早于今天，则使用截止日期作为结束时间
	if signDeadline.Before(endTime) {
		endTime = signDeadline
	}

	// 获取该用户所有的签到记录
	var signLogs []Log
	err = LOG_DB.Where(
		"user_id = ? AND type = ?",
		userId, LogTypeSign,
	).Find(&signLogs).Error
	if err != nil {
		return nil, err
	}

	// 创建签到日期映射
	signedDates := make(map[string]bool)
	for _, log := range signLogs {
		logTime := time.Unix(log.CreatedAt, 0)
		dateStr := logTime.Format("2006-01-02")
		signedDates[dateStr] = true
	}

	// 生成签到列表
	var signList []SignStatus
	currentDate := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, startTime.Location())

	for !currentDate.After(endTime) {
		dateStr := currentDate.Format("2006-01-02")
		signList = append(signList, SignStatus{
			Date:   dateStr,
			Signed: signedDates[dateStr],
		})
		currentDate = currentDate.AddDate(0, 0, 1)
	}

	return signList, nil
}

// GetSignInfo 获取签到信息（用于前端展示）
type SignInfo struct {
	Enabled       bool         `json:"enabled"`         // 签到功能是否启用
	QuotaPerSign  int          `json:"quota_per_sign"`  // 每次签到获得的额度
	SignInDays    int          `json:"sign_in_days"`    // 允许签到的天数
	SignedToday   bool         `json:"signed_today"`    // 今天是否已签到
	CanSign       bool         `json:"can_sign"`        // 是否可以签到
	Message       string       `json:"message"`         // 提示信息
	RemainingDays int          `json:"remaining_days"`  // 剩余可签到天数
	TotalSignDays int          `json:"total_sign_days"` // 总共已签到天数
	SignList      []SignStatus `json:"sign_list"`       // 签到列表
}

// GetUserSignInfo 获取用户签到信息
func GetUserSignInfo(userId int) (*SignInfo, error) {
	info := &SignInfo{
		Enabled:      common.QuotaForSign > 0,
		QuotaPerSign: common.QuotaForSign,
		SignInDays:   common.SignInDays,
	}

	if !info.Enabled {
		info.Message = "签到功能未启用"
		return info, nil
	}

	// 获取用户信息
	user, err := GetUserById(userId, false)
	if err != nil {
		return nil, err
	}

	// 检查用户分组是否允许签到
	if !ratio_setting.IsGroupSignEnabled(user.Group) {
		info.Message = "您所在的用户组不允许签到"
		info.CanSign = false
		return info, nil
	}

	// 老用户（没有注册时间记录）不能签到
	if user.CreatedTime <= 0 {
		info.Message = "签到功能仅限新注册用户"
		info.CanSign = false
		info.RemainingDays = 0
		return info, nil
	}

	// 计算剩余可签到天数 = 允许签到天数 - 已注册天数
	registeredDays := int((common.GetTimestamp() - user.CreatedTime) / (24 * 60 * 60))
	info.RemainingDays = common.SignInDays - registeredDays
	if info.RemainingDays < 0 {
		info.RemainingDays = 0
	}

	// 获取总签到天数
	var count int64
	err = LOG_DB.Model(&Log{}).Where(
		"user_id = ? AND type = ?",
		userId, LogTypeSign,
	).Count(&count).Error
	if err != nil {
		return nil, err
	}
	info.TotalSignDays = int(count)

	// 检查今天是否已签到
	hasSignedToday, err := HasUserSignedToday(userId)
	if err != nil {
		return nil, err
	}
	info.SignedToday = hasSignedToday

	// 获取签到列表
	signList, err := GetUserSignList(userId)
	if err != nil {
		return nil, err
	}
	info.SignList = signList

	// 检查是否可以签到
	canSign, reason := CheckUserSignEligibility(userId)
	info.CanSign = canSign
	if !canSign {
		info.Message = reason
	}

	return info, nil
}
