package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCustomLogTypeValuesRemainCompatible(t *testing.T) {
	assert.Equal(t, 7, LogTypeLogin)
	assert.Equal(t, 8, LogTypeMeeting)
	assert.Equal(t, 9, LogTypeActive)
	assert.Equal(t, 10, LogTypeUnlock)
	assert.Equal(t, 11, LogTypeCheckin)
	assert.Equal(t, 12, LogTypeQuotaExpiry)
	assert.Equal(t, 13, LogTypeVoice)
}

func TestQuotaConsumeLogTypesKeepLoginSeparateFromVoice(t *testing.T) {
	tests := []struct {
		name     string
		logType  int
		expected bool
	}{
		{name: "consume", logType: LogTypeConsume, expected: true},
		{name: "voice", logType: LogTypeVoice, expected: true},
		{name: "meeting", logType: LogTypeMeeting, expected: true},
		{name: "unlock", logType: LogTypeUnlock, expected: true},
		{name: "login", logType: LogTypeLogin, expected: false},
		{name: "activity", logType: LogTypeActive, expected: false},
		{name: "checkin", logType: LogTypeCheckin, expected: false},
		{name: "quota expiry", logType: LogTypeQuotaExpiry, expected: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, IsQuotaConsumeLogType(test.logType))
		})
	}
}
