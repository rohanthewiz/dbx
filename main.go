package main

import (
	"dbx/bench"
	"dbx/cfg"
	"os"

	_ "github.com/lib/pq"
	"github.com/rohanthewiz/logger"
	"github.com/rohanthewiz/serr"
)

func main() {
	logger.InitLog(logger.LogConfig{
		LogLevel: "debug",
	})
	defer logger.CloseLog()

	opts, err := cfg.GetAndValidateCmdLineOpts()
	if err != nil {
		handleError(serr.Wrap(err, "Options validation failed"))
	}

	err = bench.ExerciseDB(opts)
	if err != nil {
		handleError(serr.Wrap(err))
	}
}

func handleError(err error) {
	logger.LogErr(err)
	os.Exit(1)
}
