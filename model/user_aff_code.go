package model

import (
	"errors"
	"strings"

	"github.com/QuantumNous/new-api/common"
	sqlitedriver "github.com/glebarez/go-sqlite"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

const affCodeMaxAttempts = 5

func isAffCodeConflict(db *gorm.DB, err error) bool {
	table := db.NamingStrategy.TableName("User")
	index := db.NamingStrategy.IndexName(table, "aff_code")
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505" && pgErr.ConstraintName == index
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1062 &&
			(strings.HasSuffix(mysqlErr.Message, " for key '"+index+"'") ||
				strings.HasSuffix(mysqlErr.Message, " for key '"+table+"."+index+"'"))
	}
	var sqliteErr *sqlitedriver.Error
	if errors.As(err, &sqliteErr) {
		// SQLITE_CONSTRAINT_UNIQUE includes the exact affected column in its message.
		return sqliteErr.Code() == 2067 && strings.HasSuffix(sqliteErr.Error(),
			"UNIQUE constraint failed: "+table+".aff_code (2067)")
	}
	return false
}

func withAffCodeRetry(db *gorm.DB, write func(tx *gorm.DB, code string) error) error {
	var err error
	for range affCodeMaxAttempts {
		code := common.GetRandomString(8)
		// Nested transactions use a savepoint so a PostgreSQL unique violation
		// does not leave the caller's registration/OAuth transaction aborted.
		err = db.Transaction(func(tx *gorm.DB) error {
			return write(tx, code)
		})
		if !isAffCodeConflict(db, err) {
			return err
		}
	}
	return err
}

// GetOrCreateUserAffCode preserves existing codes, including a code filled by
// another request after our initial read. Only the invitation column is updated.
func GetOrCreateUserAffCode(userID int) (string, error) {
	if userID <= 0 {
		return "", gorm.ErrRecordNotFound
	}
	var user User
	if err := DB.Select("aff_code").First(&user, userID).Error; err != nil {
		return "", err
	}
	if user.AffCode != "" {
		return user.AffCode, nil
	}
	err := withAffCodeRetry(DB, func(tx *gorm.DB, code string) error {
		result := tx.Model(&User{}).
			Where("id = ? AND (aff_code = ? OR aff_code IS NULL)", userID, "").
			Update("aff_code", code)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return tx.Select("aff_code").First(&user, userID).Error
		}
		user.AffCode = code
		return nil
	})
	if err != nil {
		return "", err
	}
	return user.AffCode, nil
}
