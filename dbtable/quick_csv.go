package dbtable

import "github.com/rohanthewiz/serr"

func (dbt *DBTable) QuickCSV(filename string) (err error) {
	err = dbt.WriteCSV(&FileConfig{
		FileNamePattern:     "",
		FileName:            filename,
		Delimiter:           "",
		EscapeCharacter:     "",
		IgnoreCaseForHeader: false,
		SanitizeHeader:      false,
		Limit:               0,
		NoQuotedStrings:     false,
		CreateEmptyFile:     false,
		LazyQuotesRequired:  false,
		IgnoreMissingFile:   false,
		FileCreateMode:      "",
		IgnoreHeader:        false,
	})
	if err != nil {
		return serr.Wrap(err)
	}
	return
}
