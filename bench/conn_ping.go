package bench

import (
	"database/sql"
	"dbx/cfg"
	"dbx/core/dbase"

	"github.com/rohanthewiz/logger"
	"github.com/rohanthewiz/serr"
)

func connectAndPing(opts cfg.Options) (*sql.DB, error) {
	dbCfg := dbase.ConnectionCfg{
		Host:     opts.Host,
		Port:     opts.Port,
		User:     opts.User,
		Password: opts.Password,
		DBName:   opts.DBName,
		UseSSL:   opts.UseSSL,
	}

	db, err := dbase.Connect(dbCfg)
	if err != nil {
		return nil, serr.Wrap(err, "error connecting to "+opts.DBType)
	}

	err = db.Ping()
	if err != nil {
		return nil, serr.Wrap(err, "unable to ping db", "dbType", opts.DBType, "database", dbCfg.DBName)
	}

	version, err := dbase.GetDBVersion(db)
	if err != nil {
		return nil, serr.Wrap(err, "error getting db version")
	}
	logger.Info("DB: " + version)

	return db, nil
}
