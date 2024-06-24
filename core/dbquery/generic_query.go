// package dquery provides core db query functions
package dbquery

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rohanthewiz/serr"
)

type QueryOptions struct {
	Limit int64 // Limit is how many rows to fetch
}

// DBQuery is a generic and flexible query function
func DBQuery(db *sql.DB, query string, options ...QueryOptions) (data []map[string]any,
	columnTypes []*sql.ColumnType, stats Stats, err error) {

	opts := QueryOptions{}
	if len(options) == 1 {
		opts.Limit = options[0].Limit
	}

	t0 := time.Now()

	rs, err := db.Query(query)
	t1 := time.Now()
	if err != nil {
		return data, columnTypes, stats, serr.Wrap(err, "Unable to query the DB")
	}
	defer func() {
		er := rs.Close()
		if er != nil {
			fmt.Println("Error closing recordset -", serr.StringFromErr(serr.Wrap(er)))
		}
	}()

	cols, err := rs.Columns()
	if err != nil {
		return data, columnTypes, stats, serr.Wrap(err, "Unable to obtain column names from recordset")
	}

	columnTypes, _ = rs.ColumnTypes()

	rowData := make([]any, 0, len(cols))
	values := make([]any, len(cols))

	for i := range cols {
		rowData = append(rowData, &values[i])
	}

	count := int64(0)
	for rs.Next() {
		err = rs.Scan(rowData...)
		if err != nil {
			return data, columnTypes, stats, serr.Wrap(err, "Unable to scan the DB")
		}

		row := make(map[string]any)
		for i, col := range cols {
			row[col] = fmt.Sprintf("%v", values[i])
		}
		data = append(data, row)
		count++

		if opts.Limit > 0 && count >= opts.Limit {
			break
		}
	}

	t2 := time.Now()

	stats.RowsCount = count
	stats.Timing.QueryTime = t1.Sub(t0)
	stats.Timing.FetchTime = t2.Sub(t1)
	stats.Timing.Total = t2.Sub(t0)

	return
}
