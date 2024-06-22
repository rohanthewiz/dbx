package queryops

import (
	"database/sql"
	"dbx/dbquery"
	"dbx/utils/strutils"
	"fmt"
	"github.com/rohanthewiz/serr"
)

// execQuery can be used when we don't care about any data returned
func ExecQuery(db *sql.DB, qry string) (err error) {
	_, _, _, err = dbquery.DBQuery(db, qry)
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
