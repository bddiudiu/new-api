package operation_setting

import (
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/config"
)

// A century bounds calendar arithmetic while allowing long-lived grants.
const MaxQuotaExpiryDays = 36500

type LogTypeExpiryRule struct {
	Label      string `json:"label"`
	LogType    int    `json:"log_type"`
	ExpireDays int    `json:"expire_days"`
}

type QuotaExpirySetting struct {
	Rules []LogTypeExpiryRule `json:"rules"`
}

var quotaExpirySetting = QuotaExpirySetting{
	Rules: []LogTypeExpiryRule{},
}

func init() {
	config.GlobalConfig.Register("quota_expiry_setting", &quotaExpirySetting)
}

func GetQuotaExpirySetting() *QuotaExpirySetting {
	return &quotaExpirySetting
}

func GetExpireDaysForLogType(logType int) int {
	common.OptionMapRWMutex.RLock()
	defer common.OptionMapRWMutex.RUnlock()
	for _, rule := range quotaExpirySetting.Rules {
		if rule.LogType == logType && rule.ExpireDays > 0 && rule.ExpireDays <= MaxQuotaExpiryDays {
			return rule.ExpireDays
		}
	}
	return 0
}

func GetLogTypeExpireDaysMap() map[int]int {
	common.OptionMapRWMutex.RLock()
	defer common.OptionMapRWMutex.RUnlock()
	result := make(map[int]int, len(quotaExpirySetting.Rules))
	for _, rule := range quotaExpirySetting.Rules {
		if rule.ExpireDays > 0 && rule.ExpireDays <= MaxQuotaExpiryDays && result[rule.LogType] == 0 {
			result[rule.LogType] = rule.ExpireDays
		}
	}
	return result
}
