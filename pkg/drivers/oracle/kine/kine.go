package kine

import (
	"regexp"
	"strings"

	"github.com/AdamShannag/ora-kine/pkg/util"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type OraErrorCode string

const (
	UniqueViolation         OraErrorCode = "ORA-00001"
	NameAlreadyInUse        OraErrorCode = "ORA-00955"
	TableOrViewDoesNotExist OraErrorCode = "ORA-00942"
)

func ErrorIs(err error, code OraErrorCode) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), string(code))
}

type Kine struct {
	ID             int64  `gorm:"primary_key;type:INTEGER not null"`
	Name           string `gorm:"type:VARCHAR2(630)"`
	Created        int64  `gorm:"type:INTEGER"`
	Deleted        int64  `gorm:"type:INTEGER"`
	CreateRevision int64  `gorm:"type:INTEGER"`
	PrevRevision   int64  `gorm:"type:INTEGER"`
	Lease          int64  `gorm:"type:INTEGER"`
	Value          []byte `gorm:"type:RAW(2000);create:false"`
	OldValue       []byte `gorm:"type:RAW(2000);create:false"`
}

func (Kine) TableName() string {
	return "KINE"
}

var indexMap = map[string]string{
	"KINE_NAME_INDEX":                "CREATE INDEX KINE_NAME_INDEX ON KINE (NAME)",
	"KINE_NAME_ID_INDEX":             "CREATE INDEX KINE_NAME_ID_INDEX ON KINE (NAME, ID)",
	"KINE_ID_DELETED_INDEX":          "CREATE INDEX KINE_ID_DELETED_INDEX ON KINE (ID, DELETED)",
	"KINE_PREV_REVISION_INDEX":       "CREATE INDEX KINE_PREV_REVISION_INDEX ON KINE (PREV_REVISION)",
	"KINE_NAME_PREV_REVISION_UINDEX": "CREATE UNIQUE INDEX KINE_NAME_PREV_REVISION_UINDEX ON KINE (NAME, PREV_REVISION)",
}

func (k *Kine) Setup(db *gorm.DB) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")

	if !k.tableExists(db, "KINE") {
		db.Migrator().CreateTable(&Kine{})

		var count int64
		db.Raw("SELECT COUNT(*) FROM user_sequences WHERE sequence_name = 'KINE_SEQ'").Count(&count)
		if count == 0 {
			err := db.Exec(`CREATE SEQUENCE KINE_SEQ START WITH 1 INCREMENT BY 1`).Error
			if err != nil {
				return err
			}
		}

		err := db.Exec(`CREATE OR REPLACE TRIGGER KINE_TRIGGER BEFORE INSERT ON KINE FOR EACH ROW
		BEGIN
		SELECT KINE_SEQ.NEXTVAL INTO :NEW.ID FROM DUAL;
		END;`).Error

		if err != nil {
			return err
		}
	}

	for index, sql := range indexMap {
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(sql))
		var count int64
		db.Raw("SELECT COUNT(*) FROM user_indexes WHERE table_name = ? AND index_name = ?", "KINE", index).Count(&count)

		if count == 0 {
			err := db.Exec(sql).Error
			if err != nil {
				return err
			}
		}
	}
	logrus.Infof("Database tables and indexes are up to date")

	return nil
}

func (*Kine) tableExists(db *gorm.DB, tableName string) bool {
	var count int64
	db.Raw("SELECT COUNT(*) FROM user_tables WHERE table_name = ?", tableName).Scan(&count)
	return count > 0
}

func ExtractErrorCode(err error) string {
	pattern := regexp.MustCompile(`ORA-\d{5}`)
	match := pattern.FindString(err.Error())

	if match != "" {
		return match
	}

	return "-999"
}
