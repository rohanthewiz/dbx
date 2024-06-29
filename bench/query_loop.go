package bench

import (
	"database/sql"
	"dbx/core/dbquery"
	"dbx/core/dbtable"
	"dbx/report"
	"fmt"
	"time"

	"github.com/rohanthewiz/serr"
)

func RunQueryLoop(dbType string, columnar bool, query string, db *sql.DB, queryDescr string,
	statsTbl *dbtable.DBTable, runs int) (err error) {
	const pauseTime = 1 * time.Second

	fmt.Println("### Running Query loop")
	for i := 0; i < runs; i++ {
		_, stats, err := dbquery.QueryResultsAsDBTable(db, query)
		if err != nil {
			return serr.Wrap(err, "error querying "+dbType)
		}

		err = report.AppendRunStats(dbType, columnar, queryDescr, i+1, statsTbl, stats)
		if err != nil {
			return serr.Wrap(err, "error appending to Run stats "+dbType)
		}
		time.Sleep(pauseTime)
	}

	return
}
