package main

import (
	"dbx/bench"
	"dbx/cfg"
	_ "github.com/lib/pq"
	"github.com/rohanthewiz/logger"
	"github.com/rohanthewiz/serr"
	"os"
	"strings"
)

func main() {
	opts, err := getAndValidateOptions()
	if err != nil {
		handleError(serr.Wrap(err, "Options validation failed"))
	}

	err = bench.ExerciseDB(opts)
	if err != nil {
		handleError(serr.Wrap(err))
	}
}

func getAndValidateOptions() (opts cfg.Options, err error) {
	opts = cfg.GetOptions()

	switch opts.DBType {
	case cfg.AlloyDBtype:
	case cfg.PGDBType:
	default:
		return opts, serr.New("You must specify a known db type", "options",
			strings.Join([]string{cfg.PGDBType, cfg.AlloyDBtype}, ", "))
	}
	return
}

func handleError(err error) {
	logger.LogErr(err)
	os.Exit(1)
}
