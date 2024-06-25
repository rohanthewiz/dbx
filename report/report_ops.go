package report

import (
	"dbx/core/dbquery"
	"dbx/core/dbtable"

	"github.com/rohanthewiz/serr"
)

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
