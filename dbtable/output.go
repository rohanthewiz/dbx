package dbtable

import (
	"fmt"
	"github.com/rohanthewiz/serr"
	"os"
	"strings"
	"time"
)

const (
	FILE_CREATE_RDONLY = "rdonly"
	FILE_CREATE_WRONLY = "wronly"
	FILE_CREATE_RDWR   = "rdwr"
	FILE_CREATE_APPEND = "append"
)

// FileConfig ... structure to store files info
type FileConfig struct {
	FileNamePattern     string //Only for CSV reading: instead of exact name we can use pattern. Pattern supposed to find one file only
	FileName            string //Name of File which was supposed to read/write. If pattern is provided while reading for csv, then FileName should be empty
	Delimiter           string //Delimter to read/write file
	EscapeCharacter     string
	IgnoreCaseForHeader bool   //Optional: while reading, converts header values to lower case cols in dbtbl
	SanitizeHeader      bool   //Optional: For csv files, headernames will be sanitized
	Limit               int64  //Optional: while reading, this will be used to read top n records from file
	NoQuotedStrings     bool   //While creating if value is false, text type values will be written without quotes (")
	CreateEmptyFile     bool   //Optional: while writing if this flag enabled it will write cols as headers even rows are 0
	LazyQuotesRequired  bool   //If LazyQuotes is true, a quote may appear in an unquoted field and a non-doubled quote may appear in a quoted field.
	IgnoreMissingFile   bool   //Optional: While reading CSV file, if value is true, then reader plugin won't fail if file not present
	FileCreateMode      string //Optional: Default: Writeonly. while writing will be used if needed to
	IgnoreHeader        bool   //Optional: Default: false. while writing, if set true, WriterPlugin will not create header using columns
}

func (dbt *DBTable) WriteCSV(cfg *FileConfig) (err error) {
	/*	if IsTableEmpty(dbTbl) && !cfg.CreateEmptyFile && dbTbl != nil {
			log.Error("Table is empty or nil", log.Pairs{"file": cfg.FileName})
			return
		}
	*/
	var f *os.File
	switch strings.ToLower(cfg.FileCreateMode) {
	case FILE_CREATE_APPEND:
		// logger.Info("File being created in Append mode", log.Pairs{"filename": cfg.FileName})
		f, err = os.OpenFile(cfg.FileName, os.O_WRONLY|os.O_APPEND, 0644)
		// case FILE_CREATE_WRONLY:
		// log.Info("File being created in Writeonly mode", log.Pairs{"filename": cfg.FileName})
		// f, err = os.OpenFile(cfg.FileName, os.O_WRONLY, 0600)
	default:
		// log.Info("File being created in Writeonly mode", log.Pairs{"filename": cfg.FileName})
		f, err = os.OpenFile(cfg.FileName, os.O_WRONLY, 0600)
	}

	if os.IsNotExist(err) {
		f, err = os.Create(cfg.FileName)
	}
	if err != nil {
		return serr.Wrap(err)
	}
	defer f.Close()

	if cfg.Delimiter == "" {
		cfg.Delimiter = ","
	}

	//checking for backward compatibility
	if !cfg.IgnoreHeader {
		_, err = f.WriteString(strings.Join(dbt.Cols, cfg.Delimiter) + "\n")
		if err != nil {
			return serr.Wrap(err, "error writing header data")
		}

	} else {
		// log.Info("IgnoreHeader set to true. Ignored columns in header.")
	}

	colCount := len(dbt.Cols)

	// TODO: Limit
	for _, v := range dbt.Rows {
		var rowData string
		for i, s := range dbt.Cols {
			if v[s] == nil {
				if !cfg.NoQuotedStrings {
					rowData += "\"\""
				} else {
					rowData += ""
				}
			} else {
				switch val := v[s].(type) {
				case string:
					if !cfg.NoQuotedStrings {
						rowData += fmt.Sprintf("\"%v\"", strings.Replace(v[s].(string), "\"", "\\\"", -1))
					} else {
						rowData += fmt.Sprintf("%v", v[s].(string))
					}
				case time.Time:
					tm := v[s].(time.Time) // "YYYY-MM-DD hh:mm:ss ZZZ"
					dt := tm.Format("2006-01-02 15:04:05 MST")
					if !cfg.NoQuotedStrings {
						rowData += fmt.Sprintf("\"%v\"", dt)
					} else {
						rowData += fmt.Sprintf("%v", dt)
					}
				case time.Duration:
					if !cfg.NoQuotedStrings {
						rowData += fmt.Sprintf("\"%d\"", val.Milliseconds())
					} else {
						rowData += fmt.Sprintf("%d", val.Milliseconds())
					}

				default:
					if !cfg.NoQuotedStrings {
						rowData += fmt.Sprintf("\"%v\"", v[s])
					} else {
						rowData += fmt.Sprintf("%v", v[s])
					}
				}
			}

			if i < colCount-1 {
				rowData += cfg.Delimiter
			} else {
				rowData += "\n"
			}
		}

		_, err = f.WriteString(rowData)
		if err != nil {
			return serr.Wrap(err, "error writing row data")
		}
	}

	return
}
