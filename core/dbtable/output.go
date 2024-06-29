package dbtable

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rohanthewiz/serr"
)

const (
	FILE_CREATE_RDONLY = "rdonly"
	FILE_CREATE_WRONLY = "wronly"
	FILE_CREATE_RDWR   = "rdwr"
	FILE_CREATE_APPEND = "append"
)

// FileConfig ... structure to store files info
type FileConfig struct {
	FileNamePattern string // Instead of FileName we can use a pattern
	FileName        string // File name to read/write
	Delimiter       string
	Limit           int64  // take first n records
	NoQuotedStrings bool   // Indicates whether or not text values should be quoted
	CreateEmptyFile bool   // Optional: will write headers only when there are no rows
	FileCreateMode  string // Optional: Default: FILE_CREATE_RDWR
	IgnoreHeader    bool
}

func (dbt *DBTable) WriteCSV(cfg *FileConfig) (err error) {
	/*	if IsTableEmpty(dbTbl) && !cfg.CreateEmptyFile {
			return serr.Wrap("The table is empty")
		}
	*/
	var f *os.File
	switch strings.ToLower(cfg.FileCreateMode) {
	case FILE_CREATE_APPEND:
		f, err = os.OpenFile(cfg.FileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	case FILE_CREATE_WRONLY:
		f, err = os.OpenFile(cfg.FileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	default:
		// log.Info("File being created in Writeonly mode", log.Pairs{"filename": cfg.FileName})
		f, err = os.OpenFile(cfg.FileName, os.O_CREATE|os.O_RDWR, 0644)
	}

	if os.IsNotExist(err) {
		f, err = os.Create(cfg.FileName)
	}
	if err != nil {
		return serr.Wrap(err)
	}
	defer func() {
		_ = f.Close()
	}()

	if cfg.Delimiter == "" {
		cfg.Delimiter = ","
	}

	// checking for backward compatibility
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
