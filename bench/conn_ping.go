package bench

import (
	"database/sql"
	"dbx/cfg"
	"dbx/dbase"

	"github.com/rohanthewiz/serr"
)

func connectAndPing(dbType string, port string, err error) (*sql.DB, error) {
	var dbCfg dbase.ConnectionCfg

	if dbType == cfg.AlloyDBtype {
		dbCfg = dbase.ConnectionCfg{
			Host:     "localhost",
			Port:     port,
			User:     "gwsadmin",
			Password: "adminer",
			DBName:   "myDB",
		}
	} else {
		dbCfg = dbase.ConnectionCfg{
			Host:     "localhost",
			Port:     port,
			User:     "gws",
			Password: "tester",
			DBName:   "myDB",
		}
	}

	db, err := dbase.Connect(dbCfg)
	if err != nil {
		return nil, serr.Wrap(err, "error connecting to "+dbType)
	}

	err = db.Ping()
	if err != nil {
		return nil, serr.Wrap(err, "unable to ping db", "dbType", dbType, "database", dbCfg.DBName)
	}
	return db, nil
}
