package dbtable

import "github.com/rohanthewiz/serr"

func (dbt *DBTable) QuickCSV(filename string) (err error) {
	err = dbt.WriteCSV(&FileConfig{
		FileName: filename,
		Limit:    0,
	})
	if err != nil {
		return serr.Wrap(err)
	}
	return
}
