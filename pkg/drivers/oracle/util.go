package oracle

import (
	"database/sql"
	"net/url"

	"github.com/AdamShannag/ora-kine/pkg/drivers/generic"
	"github.com/AdamShannag/ora-kine/pkg/tls"
	oracle "github.com/godoes/gorm-oracle"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	defaultMaxIdleConns = 2 // copied from database/sql
)

func openAndTest(dataSourceName string) (*gorm.DB, *sql.DB, error) {
	gormDB, err := gorm.Open(oracle.Open(dataSourceName), &gorm.Config{})
	if err != nil {
		return nil, nil, err
	}

	db, err := gormDB.DB()

	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, nil, err
		}
	}

	if err != nil {
		return nil, nil, err
	}
	return gormDB, db, nil
}

func configureConnectionPooling(connPoolConfig generic.ConnectionPoolConfig, db *sql.DB) {
	// behavior copied from database/sql - zero means defaultMaxIdleConns; negative means 0
	if connPoolConfig.MaxIdle < 0 {
		connPoolConfig.MaxIdle = 0
	} else if connPoolConfig.MaxIdle == 0 {
		connPoolConfig.MaxIdle = defaultMaxIdleConns
	}

	logrus.Infof("Configuring %s database connection pooling: maxIdleConns=%d, maxOpenConns=%d, connMaxLifetime=%s", "oracle", connPoolConfig.MaxIdle, connPoolConfig.MaxOpen, connPoolConfig.MaxLifetime)
	db.SetMaxIdleConns(connPoolConfig.MaxIdle)
	db.SetMaxOpenConns(connPoolConfig.MaxOpen)
	db.SetConnMaxLifetime(connPoolConfig.MaxLifetime)
}

func prepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	} else {
		dataSourceName = "oracle://" + dataSourceName
	}
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}
	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kubernetes"
	}

	return u.String(), nil
}
