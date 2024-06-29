package dbtable

import "github.com/rohanthewiz/serr"

func (dbt *DBTable) QuickCSV(filename string) (err error) {
	err = dbt.WriteCSV(&FileConfig{
		FileName:       filename,
		FileCreateMode: FILE_CREATE_WRONLY,
		Limit:          0,
	})
	if err != nil {
		return serr.Wrap(err)
	}
	return
}
