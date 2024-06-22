package bench

import (
	"database/sql"
	"dbx/dbtable"
	"dbx/queryops"
	"github.com/rohanthewiz/serr"
	"time"
)

func RunQueryLoop(dbType string, columnar bool, query string, db *sql.DB, queryDescr string,
	statsTbl *dbtable.DBTable, runs int) (err error) {

	const pauseTime = 1 * time.Second

	for i := 0; i < runs; i++ {
		_, stats, err := queryops.QueryResultsAsDBTable(db, query)
		if err != nil {
			return serr.Wrap(err, "error querying "+dbType)
		}

		err = queryops.AppendRunStats(dbType, columnar, queryDescr, i+1, statsTbl, stats)
		if err != nil {
			return serr.Wrap(err, "error appending to Run stats "+dbType)
		}
		time.Sleep(pauseTime)
	}

	return
}
