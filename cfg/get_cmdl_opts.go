package cfg

import (
	"fmt"
	"strings"

	"github.com/rohanthewiz/logger"
	"github.com/rohanthewiz/serr"
)

func GetAndValidateCmdLineOpts() (opts Options, err error) {
	opts = GetOptions()

	logger.Debug("Options", "opts", fmt.Sprintf("%+v", opts))
	fmt.Println()

	if opts.QueryPath = strings.TrimSpace(opts.QueryPath); opts.QueryPath == "" {
		return opts, serr.New("You must specify the path to your sql query")
	}

	if opts.NbrOfRuns < 1 {
		opts.NbrOfRuns = 1
	}

	switch opts.DBType {
	case AlloyDBtype:
	case PGDBType:
	default:
		return opts, serr.New("You must specify a known db type", "options",
			strings.Join([]string{PGDBType, AlloyDBtype}, ", "))
	}
	return
}
