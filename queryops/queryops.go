package queryops

import (
	"database/sql"
	"dbx/dbquery"
	"dbx/dbtable"
	"fmt"
	"github.com/rohanthewiz/serr"
)

func QueryResultsAsDBTable(db *sql.DB, query string) (dbt *dbtable.DBTable, stats dbquery.Stats, err error) {
	data, columnTypes, stats, err := dbquery.DBQuery(db, query)
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

func CreateStatsDBTable() (statsTbl *dbtable.DBTable) {
	cols := []string{"DB", "Columnar", "Query Type", "Run Nbr", "Query Time", "Fetch Time", "Total Time", "Total Rows"}
	return dbtable.NewDBTable(cols...)
}

func AppendRunStats(dbType string, columnarEnabled bool, queryType string, run int, statsTbl *dbtable.DBTable,
	stats dbquery.Stats) (err error) {

	err = statsTbl.AddRows([]map[string]any{
		{"DB": dbType, "Columnar": columnarEnabled,
			"Query Type": queryType, "Run Nbr": run,
			"Query Time": stats.Timing.QueryTime,
			"Fetch Time": stats.Timing.FetchTime,
			"Total Time": stats.Timing.Total,
			"Total Rows": stats.RowsCount,
		},
	})
	if err != nil {
		return serr.Wrap(err)
	}
	return
}
