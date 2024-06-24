package cfg

import (
	"flag"
	"fmt"
	"strings"
)

type Options struct {
	DBType   string
	Columnar bool
	Host     string
	Port     string
	DBName   string
	User     string
	Password string
}

// GetOptions retrieves commandline options
func GetOptions() (opts Options) {
	dbType := flag.String("dbtype", "", "database type: "+
		strings.Join([]string{PGDBType, AlloyDBtype}, ", "))
	columnar := flag.Bool("columnar", false, "use columnar engine")

	host := flag.String("host", "", "database host")
	port := flag.Int("port", 5432, "database port")
	db := flag.String("dbname", "", "database name")
	user := flag.String("user", "", "username")
	pass := flag.String("pass", "", "password")

	flag.Parse()

	opts.DBType = *dbType
	opts.Columnar = *columnar

	opts.Host = *host
	opts.DBName = *db
	opts.User = *user
	opts.Password = *pass
	opts.Port = fmt.Sprintf("%d", *port)

	return
}
