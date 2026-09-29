package model

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/relaykit/dto"

	"github.com/glebarez/sqlite"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func setupUserUpdateTestState(t *testing.T) {
	t.Helper()
	truncateTables(t)
	require.NoError(t, DB.Exec("DELETE FROM users").Error)

	oldRedisEnabled := common.RedisEnabled
	oldBatchUpdateEnabled := common.BatchUpdateEnabled
	common.RedisEnabled = false
	common.BatchUpdateEnabled = false
	t.Cleanup(func() {
		common.RedisEnabled = oldRedisEnabled
		common.BatchUpdateEnabled = oldBatchUpdateEnabled
	})
}

func createUserBindTestUser(t *testing.T) User {
	t.Helper()
	user := User{
		Username:    "bind-test-user",
		Password:    "unused-password-hash",
		Role:        common.RoleCommonUser,
		Status:      common.UserStatusEnabled,
		Group:       "default",
		AuthVersion: 1,
		AffCode:     "bind-test-aff-code",
	}
	require.NoError(t, DB.Create(&user).Error)
	return user
}

func TestUserUpdateDoesNotOverwriteConcurrentAccountingOrTokenChanges(t *testing.T) {
	setupUserUpdateTestState(t)

	user := User{
		Id:              1,
		Username:        "quota-race-user",
		Password:        "password",
		DisplayName:     "before",
		Status:          common.UserStatusEnabled,
		Quota:           1000,
		UsedQuota:       20,
		RequestCount:    3,
		AffCount:        2,
		AffQuota:        800,
		AffHistoryQuota: 1200,
	}
	user.SetAccessToken("old-token")
	require.NoError(t, DB.Create(&user).Error)

	staleUser, err := GetUserById(user.Id, true)
	require.NoError(t, err)

	require.NoError(t, DB.Model(&User{}).Where("id = ?", user.Id).Updates(map[string]any{
		"quota":         gorm.Expr("quota - ?", 400),
		"used_quota":    gorm.Expr("used_quota + ?", 400),
		"request_count": gorm.Expr("request_count + ?", 1),
		"aff_count":     gorm.Expr("aff_count + ?", 1),
		"aff_quota":     gorm.Expr("aff_quota - ?", 500),
		"aff_history":   gorm.Expr("aff_history + ?", 500),
		"access_token":  "rotated-token",
	}).Error)

	staleUser.DisplayName = "after"
	require.NoError(t, staleUser.Update(false))

	var got User
	require.NoError(t, DB.First(&got, user.Id).Error)
	assert.Equal(t, "after", got.DisplayName)
	assert.Equal(t, 600, got.Quota)
	assert.Equal(t, 420, got.UsedQuota)
	assert.Equal(t, 4, got.RequestCount)
	assert.Equal(t, 3, got.AffCount)
	assert.Equal(t, 300, got.AffQuota)
	assert.Equal(t, 1700, got.AffHistoryQuota)
	assert.Equal(t, "rotated-token", got.GetAccessToken())
}

func TestUsageAccountingSupportsSignedDirectAndBatchDeltas(t *testing.T) {
	setupUserUpdateTestState(t)
	resetBatchUpdateTestState(t)

	user := User{
		Id:           10,
		Username:     "usage-adjustment-user",
		Password:     "password",
		Status:       common.UserStatusEnabled,
		UsedQuota:    1000,
		RequestCount: 3,
	}
	channel := Channel{
		Id:        10,
		Name:      "usage-adjustment-channel",
		Key:       "sk-test",
		Status:    common.ChannelStatusEnabled,
		UsedQuota: 1000,
	}
	require.NoError(t, DB.Create(&user).Error)
	require.NoError(t, DB.Create(&channel).Error)

	UpdateUserUsedQuota(user.Id, -200)
	UpdateUserUsedQuota(user.Id, 50)
	UpdateChannelUsedQuota(channel.Id, -200)
	UpdateChannelUsedQuota(channel.Id, 50)

	var got User
	require.NoError(t, DB.Select("used_quota", "request_count").First(&got, user.Id).Error)
	assert.Equal(t, 850, got.UsedQuota)
	assert.Equal(t, 3, got.RequestCount)
	var gotChannel Channel
	require.NoError(t, DB.Select("used_quota").First(&gotChannel, channel.Id).Error)
	assert.Equal(t, int64(850), gotChannel.UsedQuota)

	common.BatchUpdateEnabled = true
	UpdateUserUsedQuota(user.Id, 400)
	UpdateUserUsedQuota(user.Id, -100)
	UpdateChannelUsedQuota(channel.Id, 400)
	UpdateChannelUsedQuota(channel.Id, -100)

	require.NoError(t, DB.Select("used_quota", "request_count").First(&got, user.Id).Error)
	assert.Equal(t, 850, got.UsedQuota, "batch deltas must remain queued until flush")
	assert.Equal(t, 3, got.RequestCount)
	require.NoError(t, DB.Select("used_quota").First(&gotChannel, channel.Id).Error)
	assert.Equal(t, int64(850), gotChannel.UsedQuota, "batch deltas must remain queued until flush")

	batchUpdate()
	require.NoError(t, DB.Select("used_quota", "request_count").First(&got, user.Id).Error)
	assert.Equal(t, 1150, got.UsedQuota)
	assert.Equal(t, 3, got.RequestCount)
	require.NoError(t, DB.Select("used_quota").First(&gotChannel, channel.Id).Error)
	assert.Equal(t, int64(1150), gotChannel.UsedQuota)
}

func TestUpdateUserAccessTokenOnlyUpdatesAccessToken(t *testing.T) {
	setupUserUpdateTestState(t)

	user := User{
		Id:              2,
		Username:        "token-rotation-user",
		Password:        "password",
		DisplayName:     "before",
		Status:          common.UserStatusEnabled,
		Quota:           1000,
		AffQuota:        800,
		AffHistoryQuota: 1200,
	}
	require.NoError(t, DB.Create(&user).Error)

	require.NoError(t, DB.Model(&User{}).Where("id = ?", user.Id).Updates(map[string]any{
		"quota":        gorm.Expr("quota + ?", 500),
		"aff_quota":    gorm.Expr("aff_quota - ?", 500),
		"display_name": "concurrent-update",
	}).Error)

	require.NoError(t, UpdateUserAccessToken(user.Id, "rotated-token"))

	var got User
	require.NoError(t, DB.First(&got, user.Id).Error)
	assert.Equal(t, "rotated-token", got.GetAccessToken())
	assert.Equal(t, "concurrent-update", got.DisplayName)
	assert.Equal(t, 1500, got.Quota)
	assert.Equal(t, 300, got.AffQuota)
	assert.Equal(t, 1200, got.AffHistoryQuota)
}

func TestUpdateUserAccessTokenRejectsSoftDeletedUser(t *testing.T) {
	setupUserUpdateTestState(t)

	user := User{
		Id:       3,
		Username: "deleted-token-rotation-user",
		Password: "password",
		Status:   common.UserStatusEnabled,
	}
	user.SetAccessToken("old-token")
	require.NoError(t, DB.Create(&user).Error)
	require.NoError(t, DB.Delete(&user).Error)

	err := UpdateUserAccessToken(user.Id, "orphaned-token")
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)

	var got User
	require.NoError(t, DB.Unscoped().First(&got, user.Id).Error)
	assert.Equal(t, "old-token", got.GetAccessToken())
}

func TestUpdateUserSettingOnlyUpdatesSetting(t *testing.T) {
	setupUserUpdateTestState(t)

	user := User{
		Id:           2,
		Username:     "setting-user",
		Password:     "password",
		Status:       common.UserStatusEnabled,
		Quota:        1000,
		UsedQuota:    20,
		RequestCount: 3,
	}
	require.NoError(t, DB.Create(&user).Error)

	require.NoError(t, DB.Model(&User{}).Where("id = ?", user.Id).Updates(map[string]any{
		"quota":         gorm.Expr("quota - ?", 250),
		"used_quota":    gorm.Expr("used_quota + ?", 250),
		"request_count": gorm.Expr("request_count + ?", 1),
	}).Error)

	require.NoError(t, UpdateUserSetting(user.Id, dto.UserSetting{Language: "zh"}))

	var got User
	require.NoError(t, DB.First(&got, user.Id).Error)
	assert.Equal(t, 750, got.Quota)
	assert.Equal(t, 270, got.UsedQuota)
	assert.Equal(t, 4, got.RequestCount)
	assert.Equal(t, "zh", got.GetSetting().Language)
}

func TestEnsureEmailAvailableRejectsExistingEmailCaseInsensitive(t *testing.T) {
	setupUserUpdateTestState(t)

	require.NoError(t, DB.Create(&User{
		Username: "existing",
		Password: "old-password",
		Email:    "Taken@Example.com",
		Status:   common.UserStatusEnabled,
	}).Error)

	err := EnsureEmailAvailable(" taken@example.COM ", 0)
	require.ErrorIs(t, err, ErrEmailAlreadyTaken)

	user, err := GetUniqueUserByEmail("TAKEN@example.com")
	require.NoError(t, err)
	assert.Equal(t, "existing", user.Username)

	require.NoError(t, EnsureEmailAvailable("taken@example.com", user.Id))
}

func TestInsertRejectsDuplicateEmailWithoutUniqueIndex(t *testing.T) {
	setupUserUpdateTestState(t)

	require.NoError(t, DB.Create(&User{
		Username: "existing",
		Password: "old-password",
		Email:    "taken@example.com",
		Status:   common.UserStatusEnabled,
	}).Error)

	user := &User{
		Username: "oauth-user",
		Email:    "TAKEN@example.com",
		Role:     common.RoleCommonUser,
		Status:   common.UserStatusEnabled,
	}

	err := user.Insert(0)
	require.ErrorIs(t, err, ErrEmailAlreadyTaken)

	var count int64
	require.NoError(t, DB.Model(&User{}).Where("username = ?", "oauth-user").Count(&count).Error)
	assert.Zero(t, count)
}

func TestInsertKeepsBlankPasswordForPasswordlessUser(t *testing.T) {
	setupUserUpdateTestState(t)

	user := &User{
		Username: "passwordless-user",
		Role:     common.RoleCommonUser,
		Status:   common.UserStatusEnabled,
	}

	require.NoError(t, user.Insert(0))

	var stored User
	require.NoError(t, DB.Where("username = ?", user.Username).First(&stored).Error)
	assert.Empty(t, stored.Password)
}

func TestUpdateUserBindColumnOnlyTouchesTheBindingColumn(t *testing.T) {
	truncateTables(t)

	user := createUserBindTestUser(t)
	require.NoError(t, DB.Model(&User{}).Where("id = ?", user.Id).Updates(map[string]any{
		"role":   common.RoleAdminUser,
		"status": common.UserStatusEnabled,
		"group":  "vip",
	}).Error)

	require.NoError(t, UpdateUserBindColumn(user.Id, "github_id", "gh-12345"))

	reloaded, err := GetUserById(user.Id, true)
	require.NoError(t, err)
	assert.Equal(t, "gh-12345", reloaded.GitHubId)
	assert.Equal(t, common.RoleAdminUser, reloaded.Role)
	assert.Equal(t, common.UserStatusEnabled, reloaded.Status)
	assert.Equal(t, "vip", reloaded.Group)
}

func TestUpdateUserBindColumnPreservesRestrictiveChange(t *testing.T) {
	truncateTables(t)

	user := createUserBindTestUser(t)
	require.NoError(t, DB.Model(&User{}).Where("id = ?", user.Id).
		Update("status", common.UserStatusDisabled).Error)
	require.NoError(t, UpdateUserBindColumn(user.Id, "wechat_id", "wx-open-id"))

	reloaded, err := GetUserById(user.Id, true)
	require.NoError(t, err)
	assert.Equal(t, "wx-open-id", reloaded.WeChatId)
	assert.Equal(t, common.UserStatusDisabled, reloaded.Status)
}

func TestUpdateUserBindColumnRejectsNonWhitelistedColumns(t *testing.T) {
	truncateTables(t)

	user := createUserBindTestUser(t)
	for _, column := range []string{"role", "status", "group", "quota", "username", "password", "id"} {
		assert.Error(t, UpdateUserBindColumn(user.Id, column, "1"), "column %s must be rejected", column)
	}
	assert.Error(t, UpdateUserBindColumn(user.Id, "github_id; DROP TABLE users", "x"))
	assert.Error(t, UpdateUserBindColumn(0, "github_id", "x"))
}

func TestValidateAndFillRejectsPasswordlessUser(t *testing.T) {
	setupUserUpdateTestState(t)

	require.NoError(t, DB.Create(&User{
		Username: "passwordless-user",
		Password: "",
		Status:   common.UserStatusEnabled,
	}).Error)

	loginUser := User{
		Username: "passwordless-user",
		Password: "NewPassword123",
	}
	err := loginUser.ValidateAndFill()
	require.ErrorIs(t, err, ErrInvalidCredentials)

	var stored User
	require.NoError(t, DB.Where("username = ?", "passwordless-user").First(&stored).Error)
	assert.Empty(t, stored.Password)
}

func TestResetUserPasswordByEmailRequiresSingleActiveMatch(t *testing.T) {
	setupUserUpdateTestState(t)

	require.NoError(t, DB.Create(&User{
		Username: "duplicate-1",
		Password: "old-1",
		Email:    "legacy@example.com",
		AffCode:  "dupe1",
		Status:   common.UserStatusEnabled,
	}).Error)
	require.NoError(t, DB.Create(&User{
		Username: "duplicate-2",
		Password: "old-2",
		Email:    "LEGACY@example.com",
		AffCode:  "dupe2",
		Status:   common.UserStatusEnabled,
	}).Error)

	err := ResetUserPasswordByEmail("legacy@example.com", "NewPassword123")
	require.ErrorIs(t, err, ErrEmailAmbiguous)

	var duplicates []User
	require.NoError(t, DB.Where("LOWER(email) = ?", "legacy@example.com").Order("username asc").Find(&duplicates).Error)
	require.Len(t, duplicates, 2)
	assert.Equal(t, "old-1", duplicates[0].Password)
	assert.Equal(t, "old-2", duplicates[1].Password)

	require.NoError(t, DB.Create(&User{
		Username: "unique",
		Password: "old",
		Email:    "unique@example.com",
		AffCode:  "unique",
		Status:   common.UserStatusEnabled,
	}).Error)

	require.NoError(t, ResetUserPasswordByEmail("UNIQUE@example.com", "NewPassword123"))

	var unique User
	require.NoError(t, DB.Where("username = ?", "unique").First(&unique).Error)
	assert.True(t, common.ValidatePasswordAndHash("NewPassword123", unique.Password))

	err = ResetUserPasswordByEmail("missing@example.com", "NewPassword123")
	require.True(t, errors.Is(err, ErrEmailNotFound))
}

func setupUserAffCodeDatabase(t *testing.T, dialect string) *gorm.DB {
	t.Helper()
	var driver gorm.Dialector
	switch dialect {
	case "sqlite":
		driver = sqlite.Open(filepath.Join(t.TempDir(), "aff-code.db") +
			"?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_txlock=immediate")
	case "mysql":
		dsn := os.Getenv("TEST_MYSQL_DSN")
		if dsn == "" {
			t.Skip("TEST_MYSQL_DSN not configured")
		}
		driver = mysql.Open(dsn)
	case "postgres":
		dsn := os.Getenv("TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("TEST_POSTGRES_DSN not configured")
		}
		driver = postgres.New(postgres.Config{DSN: dsn, PreferSimpleProtocol: true})
	}
	config := newGormConfig(false)
	config.NamingStrategy = schema.NamingStrategy{TablePrefix: "aff_code_test_"}
	db, err := gorm.Open(driver, config)
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(4)
	previousDB, previousType, previousRedis := DB, common.MainDatabaseType(), common.RedisEnabled
	DB = db
	common.SetMainDatabaseType(common.DatabaseType(dialect))
	common.RedisEnabled = false
	initCol()
	t.Cleanup(func() {
		assert.NoError(t, db.Migrator().DropTable(&User{}))
		assert.NoError(t, sqlDB.Close())
		DB = previousDB
		common.SetMainDatabaseType(previousType)
		common.RedisEnabled = previousRedis
		initCol()
	})
	require.NoError(t, db.AutoMigrate(&User{}))
	var version string
	query := "SELECT version()"
	if dialect == "sqlite" {
		query = "SELECT sqlite_version()"
	}
	require.NoError(t, db.Raw(query).Scan(&version).Error)
	t.Logf("database version: %s", version)
	return db
}

func TestAffCodeConflictRequiresUniqueConstraintTarget(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "mysql_unqualified_index", err: &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry 'code1234' for key 'idx_users_aff_code'"}, want: true},
		{name: "mysql_qualified_index", err: &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry 'code1234' for key 'users.idx_users_aff_code'"}, want: true},
		{name: "mysql_index_in_value", err: &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry 'idx_users_aff_code' for key 'users.uni_users_username'"}},
		{name: "mysql_other_error", err: &mysqldriver.MySQLError{Number: 1064, Message: "syntax error for key 'idx_users_aff_code'"}},
		{name: "postgres_index", err: &pgconn.PgError{Code: "23505", ConstraintName: "idx_users_aff_code"}, want: true},
		{name: "postgres_other_index", err: &pgconn.PgError{Code: "23505", ConstraintName: "idx_users_access_token", Detail: "idx_users_aff_code"}},
		{name: "postgres_other_error", err: &pgconn.PgError{Code: "23503", ConstraintName: "idx_users_aff_code"}},
		{name: "untyped_error", err: errors.New("Duplicate entry 'code1234' for key 'idx_users_aff_code'")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isAffCodeConflict(DB, fmt.Errorf("write failed: %w", tc.err)))
		})
	}
}

func TestUserAffCodeDatabaseMatrix(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			db := setupUserAffCodeDatabase(t, dialect)
			for _, entry := range []string{"insert", "insert_with_tx", "backfill"} {
				for _, tc := range []struct {
					name        string
					collisions  int
					otherError  bool
					uniqueField string
				}{
					{name: "new_code"},
					{name: "retry_collision", collisions: 1},
					{name: "retry_limit", collisions: 5},
					{name: "other_error", otherError: true},
					{name: "username_conflict", uniqueField: "username"},
					{name: "access_token_conflict", uniqueField: "access_token"},
				} {
					if entry == "backfill" && tc.uniqueField != "" {
						continue // Backfill only writes aff_code.
					}
					t.Run(entry+"/"+tc.name, func(t *testing.T) {
						require.NoError(t, db.Session(&gorm.Session{AllowGlobalUpdate: true}).Unscoped().Delete(&User{}).Error)
						token := "existing-test-token"
						existing := User{Username: "existing", AffCode: "taken123", AccessToken: &token}
						require.NoError(t, db.Create(&existing).Error)
						user := User{Username: "new-user", Password: "NewPassword123", AuthVersion: 7}
						if tc.uniqueField == "username" {
							user.Username = existing.Username
						}
						if tc.uniqueField == "access_token" {
							user.AccessToken = &token
						}
						if entry == "backfill" {
							require.NoError(t, db.Create(&user).Error)
						}
						before := user
						attempts := 0
						writeErr := errors.New("write unavailable")
						callback := func(tx *gorm.DB) {
							attempts++
							if entry == "backfill" {
								assert.Len(t, tx.Statement.Dest.(map[string]any)["aff_code"], 8)
							} else {
								assert.Len(t, tx.Statement.Dest.(*User).AffCode, 8)
							}
							if tc.otherError {
								tx.AddError(writeErr)
							} else if attempts <= tc.collisions {
								tx.Statement.SetColumn("aff_code", existing.AffCode)
							}
						}
						if entry == "backfill" {
							require.NoError(t, db.Callback().Update().Before("gorm:update").Register("test:aff_code", callback))
							t.Cleanup(func() { assert.NoError(t, db.Callback().Update().Remove("test:aff_code")) })
						} else {
							require.NoError(t, db.Callback().Create().Before("gorm:create").Register("test:aff_code", callback))
							t.Cleanup(func() { assert.NoError(t, db.Callback().Create().Remove("test:aff_code")) })
						}
						var err error
						var code string
						switch entry {
						case "insert":
							err = user.Insert(0)
							code = user.AffCode
						case "insert_with_tx":
							err = db.Transaction(func(tx *gorm.DB) error {
								if err := user.InsertWithTx(tx, 0); err != nil {
									return err
								}
								// The outer transaction must remain usable after a collision.
								return tx.Model(&user).Update("display_name", "continued").Error
							})
							code = user.AffCode
						case "backfill":
							code, err = GetOrCreateUserAffCode(user.Id)
						}
						wantAttempts := min(tc.collisions+1, 5)
						assert.Equal(t, wantAttempts, attempts)
						if tc.otherError || tc.uniqueField != "" || tc.collisions == 5 {
							require.Error(t, err)
							if tc.otherError {
								assert.ErrorIs(t, err, writeErr)
							} else {
								assert.Equal(t, tc.collisions == 5, isAffCodeConflict(db, err))
							}
							var count int64
							require.NoError(t, db.Model(&User{}).Count(&count).Error)
							if entry == "backfill" {
								assert.EqualValues(t, 2, count)
								var stored User
								require.NoError(t, db.First(&stored, user.Id).Error)
								assert.Equal(t, before, stored)
							} else {
								assert.EqualValues(t, 1, count)
							}
							return
						}
						require.NoError(t, err)
						assert.Len(t, code, 8)
						var stored User
						require.NoError(t, db.First(&stored, user.Id).Error)
						assert.Equal(t, code, stored.AffCode)
						if entry == "backfill" {
							before.AffCode = code
							assert.Equal(t, before, stored)
						} else {
							assert.True(t, common.ValidatePasswordAndHash("NewPassword123", stored.Password), "retry must not hash the password twice")
							if entry == "insert_with_tx" {
								assert.Equal(t, "continued", stored.DisplayName)
							}
						}
					})
				}
			}

			t.Run("preserve_existing_code", func(t *testing.T) {
				user := User{Username: "legacy-code-user", AffCode: "old4"}
				require.NoError(t, db.Create(&user).Error)
				code, err := GetOrCreateUserAffCode(user.Id)
				require.NoError(t, err)
				assert.Equal(t, "old4", code)
				id, err := GetUserIdByAffCode("old4")
				require.NoError(t, err)
				assert.Equal(t, user.Id, id)
			})

			t.Run("backfill_null", func(t *testing.T) {
				user := User{Username: "null-code-user", AffCode: "nullable"}
				require.NoError(t, db.Create(&user).Error)
				require.NoError(t, db.Model(&user).Update("aff_code", nil).Error)
				code, err := GetOrCreateUserAffCode(user.Id)
				require.NoError(t, err)
				assert.Len(t, code, 8)
				var stored User
				require.NoError(t, db.First(&stored, user.Id).Error)
				assert.Equal(t, code, stored.AffCode)
			})

			t.Run("outer_transaction_rollback", func(t *testing.T) {
				user := User{Username: "rollback-user"}
				rollbackErr := errors.New("binding failed")
				err := db.Transaction(func(tx *gorm.DB) error {
					if err := user.InsertWithTx(tx, 0); err != nil {
						return err
					}
					return rollbackErr
				})
				require.ErrorIs(t, err, rollbackErr)
				var count int64
				require.NoError(t, db.Model(&User{}).Where("username = ?", user.Username).Count(&count).Error)
				assert.Zero(t, count)
			})

			t.Run("concurrent_backfill", func(t *testing.T) {
				require.NoError(t, db.Session(&gorm.Session{AllowGlobalUpdate: true}).Unscoped().Delete(&User{}).Error)
				user := User{Username: "concurrent-user", AuthVersion: 7, Status: common.UserStatusDisabled}
				require.NoError(t, db.Create(&user).Error)
				var reads atomic.Int32
				ready := make(chan struct{}, 2)
				release := make(chan struct{})
				require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:aff_code_read", func(tx *gorm.DB) {
					if reads.Add(1) <= 2 {
						ready <- struct{}{}
						<-release
					}
				}))
				t.Cleanup(func() { assert.NoError(t, db.Callback().Query().Remove("test:aff_code_read")) })
				type result struct {
					code string
					err  error
				}
				results := make(chan result, 2)
				for range 2 {
					go func() {
						code, err := GetOrCreateUserAffCode(user.Id)
						results <- result{code: code, err: err}
					}()
				}
				<-ready
				<-ready
				close(release)
				first, second := <-results, <-results
				require.NoError(t, first.err)
				require.NoError(t, second.err)
				assert.Len(t, first.code, 8)
				assert.Equal(t, first.code, second.code)
				var stored User
				require.NoError(t, db.First(&stored, user.Id).Error)
				user.AffCode = first.code
				assert.Equal(t, user, stored)
			})

			t.Run("missing_or_deleted_user", func(t *testing.T) {
				user := User{Username: "deleted-code-user", AffCode: "deleted1"}
				require.NoError(t, db.Create(&user).Error)
				require.NoError(t, db.Delete(&user).Error)
				for _, id := range []int{0, -1, user.Id, user.Id + 1} {
					code, err := GetOrCreateUserAffCode(id)
					assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
					assert.Empty(t, code)
				}
			})
		})
	}
}
