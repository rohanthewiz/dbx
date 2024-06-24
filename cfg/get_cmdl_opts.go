package cfg

import (
	"strings"

	"github.com/rohanthewiz/serr"
)

func GetAndValidateCmdLineOpts() (opts Options, err error) {
	opts = GetOptions()

	switch opts.DBType {
	case AlloyDBtype:
	case PGDBType:
	default:
		return opts, serr.New("You must specify a known db type", "options",
			strings.Join([]string{PGDBType, AlloyDBtype}, ", "))
	}
	return
}
