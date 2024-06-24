package dbase

import (
	"database/sql"
	"fmt"

	"github.com/rohanthewiz/logger"
	"github.com/rohanthewiz/serr"
)

type ConnectionCfg struct {
	Host, Port, User, Password, DBName string
}

const dbGeneralTypePostgres = "postgres"

func Connect(cfg ConnectionCfg) (db *sql.DB, err error) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName)

	redactedConnStr := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, "<hidden>", cfg.DBName)
	logger.Info("Connection " + redactedConnStr)

	db, err = sql.Open(dbGeneralTypePostgres, connStr)
	if err != nil {
		return db, serr.Wrap(err, "Unable to connect to the DB")
	}
	return
}

func GetDBVersion(db *sql.DB) (version string, err error) {
	rv, err := db.Query(`select version();`)
	if err != nil {
		return version, serr.Wrap(err, "Unable to obtain the DB version")
	}

	if rv.Next() {
		err = rv.Scan(&version)
		if err != nil {
			return version, serr.Wrap(err, "Unable to scan the DB version")
		}
	}
	_ = rv.Close()
	return
}
