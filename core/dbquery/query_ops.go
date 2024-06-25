package dbquery

import (
	"database/sql"
	"dbx/core/dbtable"
	"dbx/core/utils/strutils"
	"fmt"

	"github.com/rohanthewiz/serr"
)

// execQuery can be used when we don't care about any data returned
func ExecQuery(db *sql.DB, qry string) (err error) {
	_, _, _, err = DBQuery(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}
	return
}

// execQueryWithPrint can be used when we don't care about any data returned
func ExecQueryWithPrint(db *sql.DB, qry string) (err error) {
	fmt.Println("## Query: ", strutils.Truncate(qry, 120, false))

	dbt, _, err := QueryResultsAsDBTable(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}
	dbt.PrettyPrint()
	return
}

func QueryResultsAsDBTable(db *sql.DB, query string) (dbt *dbtable.DBTable, stats Stats, err error) {
	data, columnTypes, stats, err := DBQuery(db, query)
	if err != nil {
		return dbt, stats, serr.Wrap(err)
	}

	fmt.Println(stats.String())

	var cols []string
	for _, ct := range columnTypes {
		cols = append(cols, ct.Name())
	}

	dbt = dbtable.NewDBTable(cols...)
	err = dbt.AddRows(data)
	if err != nil {
		return dbt, stats, serr.Wrap(err)
	}

	return
}
