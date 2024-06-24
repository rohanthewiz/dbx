package dbtable

import (
	"fmt"
	"os"
	"sort"

	"github.com/olekukonko/tablewriter"
)

type DBTable struct {
	Rows []map[string]any `json:"rows"`
	Cols []string         `json:"cols"`
}

func NewDBTable(cols ...string) (tbl *DBTable) {
	tbl = &DBTable{}
	tbl.Cols = cols

	return
}

func (dbt *DBTable) GetCols() []string {
	return dbt.Cols
}

func (dbt *DBTable) GetRows() []map[string]any {
	return dbt.Rows
}

type AddRowsOpts struct {
	CopyData bool
}

// AddRows adds rows to the table
func (dbt *DBTable) AddRows(inRows []map[string]any, options ...AddRowsOpts) (err error) {
	if len(inRows) == 0 {
		return // not a big deal if no rows
	}

	var opts AddRowsOpts
	if len(options) > 0 {
		opts = options[0]
	}

	if opts.CopyData && len(dbt.Rows) == 0 {
		copy(dbt.Rows, inRows) // take it all in at once
	} else {
		dbt.Rows = append(dbt.Rows, inRows...)
	}

	return
}

type PrintOpts struct {
	Limit int // how many rows to print - default 10
}

// PrettyPrint Pretty print the table
func (dbt *DBTable) PrettyPrint(options ...PrintOpts) {
	if dbt == nil {
		fmt.Println("No table to print")
		return
	}

	var opts PrintOpts
	if len(options) > 0 {
		opts = options[0]
	}

	if opts.Limit == 0 {
		opts.Limit = 20
	}

	tbl := tablewriter.NewWriter(os.Stdout)
	cols := dbt.Cols
	sort.Strings(cols)

	tbl.SetHeader(cols)

	limited := false
	for n, r := range dbt.Rows {
		if n >= opts.Limit {
			limited = true
			break
		}

		row := make([]string, len(dbt.Cols))
		for i, col := range dbt.Cols {
			row[i] = fmt.Sprintf("%v", r[col])
		}
		tbl.Append(row)
	}

	if limited {
		tbl.SetCaption(true, fmt.Sprintf("%d Columns, limited to %d row(s)", len(cols), opts.Limit))
	} else {
		tbl.SetCaption(true, fmt.Sprintf("%d Columns, %d row(s)", len(cols), len(dbt.Rows)))
	}

	tbl.Render() // Send output

	fmt.Println()
	return
}
