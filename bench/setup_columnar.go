package bench

import (
	"database/sql"
	"dbx/core/dbquery"
	"fmt"

	"github.com/rohanthewiz/serr"
)

// Hmm, seems like we can't alter system like this --just use psql for now
func alterSystemForColumnar(db *sql.DB) (err error) {
	qry := `alter system set google_columnar_engine.enabled=on;
      alter system set google_columnar_engine.memory_size_in_mb=8192;
      alter system set max_parallel_workers_per_gather=2;
      alter system set max_parallel_workers=12;
      `
	_, err = db.Exec(qry)
	if err != nil {
		return serr.Wrap(err)
	}
	return
}

func preSetupColumnar(db *sql.DB) (err error) {
	qry := "SHOW google_columnar_engine.enabled"
	err = dbquery.ExecQueryWithPrint(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}

	qry = "SHOW max_parallel_workers"
	err = dbquery.ExecQueryWithPrint(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}

	qry = "SHOW max_parallel_workers_per_gather"
	err = dbquery.ExecQueryWithPrint(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}

	// Don't auto recommend
	qry = "SET google_columnar_engine.enable_columnar_scan=on;"
	err = dbquery.ExecQuery(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}

	qry = "SHOW google_columnar_engine.enable_auto_columnarization"
	err = dbquery.ExecQueryWithPrint(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}

	qry = `SELECT google_columnar_engine_add('my_schema.my_table')`
	err = dbquery.ExecQuery(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}

	return
}

func callRecommend(db *sql.DB) (err error) {
	qry := "SELECT google_columnar_engine_recommend();"
	err = dbquery.ExecQueryWithPrint(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}

	return
}

func printColumnarStats(db *sql.DB) (err error) {
	qry := `SELECT * FROM google_columnar_engine_recommend(mode => 'RECOMMEND_SIZE');`
	err = dbquery.ExecQueryWithPrint(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}

	qry = `SELECT relation_name, block_count_in_cc, total_block_count,
       block_count_in_cc=total_block_count as counts_equal, size,
    auto_refresh_trigger_count, 
    auto_refresh_failure_count as auto_refresh_fails,
    auto_refresh_recent_status as auto_refresh_status
	 	from g_columnar_relations order by 1`
	err = dbquery.ExecQueryWithPrint(db, qry)
	if err != nil {
		return serr.Wrap(err)
	}

	return
}

func IsColumnarEngineOn(db *sql.DB) (columnarOn bool, err error) {
	const qry = "SHOW google_columnar_engine.enabled"

	data, _, _, err := dbquery.DBQuery(db, qry)
	if err != nil {
		return columnarOn, serr.Wrap(err)
	}
	if len(data) != 1 {
		return columnarOn, serr.New("Invalid number of rows returned")
	}

	row := data[0]
	fld := row["google_columnar_engine.enabled"]
	fmt.Printf("Columnar engine is: %v\n", fld)
	if val, ok := fld.(string); ok {
		columnarOn = val == "on"
	}
	return
}
