package cfg

import (
	"flag"
	"strings"
)

type Options struct {
	DBType      string
	UseColumnar bool
}

func GetOptions() (opts Options) {
	dbasePtr := flag.String("db", "", "database to use: "+
		strings.Join([]string{PGDBType, AlloyDBtype}, ", "))

	useColumnarPtr := flag.Bool("col", false, "use columnar engine")

	flag.Parse()

	opts.DBType = *dbasePtr
	opts.UseColumnar = *useColumnarPtr
	return
}
