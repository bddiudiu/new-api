package model

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func TestSearchRedemptionsFiltersAndPaginates(t *testing.T) {
	require.NoError(t, DB.AutoMigrate(&Redemption{}))
	require.NoError(t, DB.Session(&gorm.Session{AllowGlobalUpdate: true}).Unscoped().Delete(&Redemption{}).Error)
	t.Cleanup(func() {
		require.NoError(t, DB.Session(&gorm.Session{AllowGlobalUpdate: true}).Unscoped().Delete(&Redemption{}).Error)
	})

	now := common.GetTimestamp()
	redemptions := []Redemption{
		{Id: 1, Name: "alpha-active", Key: "00000000000000000000000000000001", Status: common.RedemptionCodeStatusEnabled, ExpiredTime: 0},
		{Id: 2, Name: "alpha-future", Key: "00000000000000000000000000000002", Status: common.RedemptionCodeStatusEnabled, ExpiredTime: now + 3600},
		{Id: 3, Name: "alpha-expired", Key: "00000000000000000000000000000003", Status: common.RedemptionCodeStatusEnabled, ExpiredTime: now - 10},
		{Id: 4, Name: "beta-disabled", Key: "00000000000000000000000000000004", Status: common.RedemptionCodeStatusDisabled, ExpiredTime: 0},
		{Id: 5, Name: "beta-used", Key: "00000000000000000000000000000005", Status: common.RedemptionCodeStatusUsed, ExpiredTime: 0},
	}
	require.NoError(t, DB.Create(&redemptions).Error)

	tests := []struct {
		name      string
		keyword   string
		status    string
		startIdx  int
		num       int
		wantTotal int64
		wantIds   []int
	}{
		{
			name:      "no filters returns all rows",
			num:       10,
			wantTotal: 5,
			wantIds:   []int{5, 4, 3, 2, 1},
		},
		{
			name:      "keyword filters by name prefix",
			keyword:   "alpha",
			num:       10,
			wantTotal: 3,
			wantIds:   []int{3, 2, 1},
		},
		{
			name:      "enabled status excludes expired rows",
			status:    "1",
			num:       10,
			wantTotal: 2,
			wantIds:   []int{2, 1},
		},
		{
			name:      "expired status returns enabled expired rows",
			status:    "expired",
			num:       10,
			wantTotal: 1,
			wantIds:   []int{3},
		},
		{
			name:      "disabled status",
			status:    "2",
			num:       10,
			wantTotal: 1,
			wantIds:   []int{4},
		},
		{
			name:      "used status",
			status:    "3",
			num:       10,
			wantTotal: 1,
			wantIds:   []int{5},
		},
		{
			name:      "pagination keeps unpaged total",
			startIdx:  1,
			num:       2,
			wantTotal: 5,
			wantIds:   []int{4, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, total, err := SearchRedemptions(tt.keyword, tt.status, tt.startIdx, tt.num)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTotal, total)
			gotIds := make([]int, 0, len(rows))
			for _, row := range rows {
				gotIds = append(gotIds, row.Id)
			}
			assert.Equal(t, tt.wantIds, gotIds)
		})
	}
}

func setupRedeemFixture(t *testing.T, quota int) (userId int, key string) {
	t.Helper()
	require.NoError(t, DB.AutoMigrate(&Redemption{}))
	require.NoError(t, DB.Session(&gorm.Session{AllowGlobalUpdate: true}).Unscoped().Delete(&Redemption{}).Error)
	t.Cleanup(func() {
		require.NoError(t, DB.Session(&gorm.Session{AllowGlobalUpdate: true}).Unscoped().Delete(&Redemption{}).Error)
		require.NoError(t, DB.Session(&gorm.Session{AllowGlobalUpdate: true}).Unscoped().Delete(&User{}).Error)
		require.NoError(t, LOG_DB.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&Log{}).Error)
	})

	user := &User{Username: "redeem-user", Password: "password", Status: common.UserStatusEnabled, Quota: 0}
	require.NoError(t, DB.Create(user).Error)

	key = "10000000000000000000000000000001"
	redemption := &Redemption{
		Name:        "redeem-test",
		Key:         key,
		Status:      common.RedemptionCodeStatusEnabled,
		Quota:       quota,
		CreatedTime: common.GetTimestamp(),
	}
	require.NoError(t, DB.Create(redemption).Error)
	return user.Id, key
}

func setupRedeemLogDatabases(t *testing.T, dialect string) {
	t.Helper()
	var driver gorm.Dialector
	switch dialect {
	case "sqlite":
		driver = sqlite.Open(":memory:")
	case "mysql":
		dsn := os.Getenv("TEST_MYSQL_DSN")
		if dsn == "" {
			t.Skip("TEST_MYSQL_DSN is not configured")
		}
		driver = mysql.Open(dsn)
	case "postgres":
		dsn := os.Getenv("TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("TEST_POSTGRES_DSN is not configured")
		}
		driver = postgres.New(postgres.Config{DSN: dsn, PreferSimpleProtocol: true})
	default:
		t.Fatalf("unsupported test database: %s", dialect)
	}

	previousDB, previousLogDB := DB, LOG_DB
	previousMainType, previousLogType := common.MainDatabaseType(), common.LogDatabaseType()
	previousRedis := common.RedisEnabled
	t.Cleanup(func() {
		DB, LOG_DB = previousDB, previousLogDB
		common.SetDatabaseTypes(previousMainType, previousLogType)
		common.RedisEnabled = previousRedis
		initCol()
	})
	common.SetDatabaseTypes(common.DatabaseType(dialect), common.DatabaseType(dialect))
	common.RedisEnabled = false
	initCol()

	// Separate handles and tables ensure redemption logs are written to LOG_DB.
	for _, target := range []struct {
		name   string
		db     **gorm.DB
		models []any
	}{
		{"main", &DB, []any{&User{}, &Redemption{}}},
		{"log", &LOG_DB, []any{&Log{}}},
	} {
		db, err := gorm.Open(driver, &gorm.Config{NamingStrategy: schema.NamingStrategy{TablePrefix: "redeem_test_" + target.name + "_"}})
		require.NoError(t, err)
		sqlDB, err := db.DB()
		require.NoError(t, err)
		sqlDB.SetMaxOpenConns(1)
		t.Cleanup(func() {
			assert.NoError(t, db.Migrator().DropTable(target.models...))
			assert.NoError(t, sqlDB.Close())
		})
		require.NoError(t, db.AutoMigrate(target.models...))
		*target.db = db
	}
	query := "SELECT version()"
	if dialect == "sqlite" {
		query = "SELECT sqlite_version()"
	}
	var version string
	require.NoError(t, DB.Raw(query).Scan(&version).Error)
	t.Logf("database version: %s", version)
}

func TestRedeemCreditsQuotaExactlyOnce(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			setupRedeemLogDatabases(t, dialect)
			settings := operation_setting.GetGeneralSetting()
			previousSettings := *settings
			t.Cleanup(func() { *settings = previousSettings })
			settings.CustomCurrencySymbol = "TEST$"
			settings.CustomCurrencyExchangeRate = 7

			for _, tc := range []struct {
				name        string
				displayType string
				quota       int
			}{
				{"USD", operation_setting.QuotaDisplayTypeUSD, 500001},
				{"CNY", operation_setting.QuotaDisplayTypeCNY, 500001},
				{"CUSTOM", operation_setting.QuotaDisplayTypeCustom, 500001},
				{"TOKENS", operation_setting.QuotaDisplayTypeTokens, 500001},
				{"maximum_wallet_quota", operation_setting.QuotaDisplayTypeUSD, common.MaxWalletQuota},
			} {
				t.Run(tc.name, func(t *testing.T) {
					settings.QuotaDisplayType = tc.displayType
					userId, key := setupRedeemFixture(t, tc.quota)

					quota, err := Redeem(key, userId, "203.0.113.17")
					require.NoError(t, err)
					assert.Equal(t, tc.quota, quota)

					var redemption Redemption
					require.NoError(t, DB.First(&redemption, "name = ?", "redeem-test").Error)
					assert.Equal(t, common.RedemptionCodeStatusUsed, redemption.Status)
					assert.Equal(t, userId, redemption.UsedUserId)

					// Retrying must neither credit quota nor write a second success log.
					_, err = Redeem(key, userId, "203.0.113.17")
					require.Error(t, err)
					var user User
					require.NoError(t, DB.First(&user, "id = ?", userId).Error)
					assert.Equal(t, tc.quota, user.Quota)

					var logs []Log
					require.NoError(t, LOG_DB.Where("user_id = ? AND type = ?", userId, LogTypeTopup).Find(&logs).Error)
					require.Len(t, logs, 1)
					assert.Equal(t, tc.quota, logs[0].Quota)
					assert.Equal(t, fmt.Sprintf("Topped up %d with a redemption code, code ID %d", tc.quota, redemption.Id), logs[0].Content)
					var other struct {
						ContentParts []struct {
							Key    string `json:"key"`
							Params struct {
								Quota int `json:"quota"`
								Id    int `json:"id"`
							} `json:"params"`
						} `json:"content_parts"`
					}
					require.NoError(t, common.UnmarshalJsonStr(logs[0].Other, &other))
					require.Len(t, other.ContentParts, 1)
					assert.Equal(t, "Topped up {{quota}} with a redemption code, code ID {{id}}", other.ContentParts[0].Key)
					assert.Equal(t, tc.quota, other.ContentParts[0].Params.Quota)
					assert.Equal(t, redemption.Id, other.ContentParts[0].Params.Id)
				})
			}
		})
	}
}

func TestRedeemRejectsWalletOverflow(t *testing.T) {
	userId, key := setupRedeemFixture(t, 11)
	require.NoError(t, DB.Model(&User{}).Where("id = ?", userId).Update("quota", common.MaxWalletQuota-10).Error)

	_, err := Redeem(key, userId, "203.0.113.17")
	require.ErrorIs(t, err, ErrRedeemFailed)

	var user User
	require.NoError(t, DB.First(&user, "id = ?", userId).Error)
	assert.Equal(t, common.MaxWalletQuota-10, user.Quota)

	var redemption Redemption
	require.NoError(t, DB.First(&redemption, "key = ?", key).Error)
	assert.Equal(t, common.RedemptionCodeStatusEnabled, redemption.Status)
	var logCount int64
	require.NoError(t, LOG_DB.Model(&Log{}).Where("user_id = ? AND type = ?", userId, LogTypeTopup).Count(&logCount).Error)
	assert.Zero(t, logCount, "failed redemption must not write a success log")
}

func TestRedemptionQuotaRejectsWalletOverflow(t *testing.T) {
	setupRedeemFixture(t, 500)

	redemption := &Redemption{
		Name:        "overflow-redemption",
		Key:         "10000000000000000000000000000002",
		Status:      common.RedemptionCodeStatusEnabled,
		Quota:       common.MaxWalletQuota + 1,
		CreatedTime: common.GetTimestamp(),
	}
	require.Error(t, redemption.Insert())
}

// Exactly one of several concurrent redeems of the same code may win, and
// quota must be credited exactly once.
func TestRedeemConcurrentSingleSuccess(t *testing.T) {
	userId, key := setupRedeemFixture(t, 300)

	const goroutines = 5
	successes := make([]bool, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			if _, err := Redeem(key, userId, "203.0.113.17"); err == nil {
				successes[idx] = true
			}
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, ok := range successes {
		if ok {
			successCount++
		}
	}
	assert.Equal(t, 1, successCount, "exactly one concurrent redeem should succeed")

	var user User
	require.NoError(t, DB.First(&user, "id = ?", userId).Error)
	assert.Equal(t, 300, user.Quota, "quota must be credited exactly once")
}
