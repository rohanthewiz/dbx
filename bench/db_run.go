package bench

import (
	"dbx/cfg"
	"dbx/core/dbquery"
	"dbx/core/dbtable"
	"dbx/report"
	"os"

	"github.com/rohanthewiz/serr"
)

const explainPrefix = "EXPLAIN (ANALYZE,COSTS OFF,BUFFERS,TIMING OFF,SUMMARY OFF) "

func ExerciseDB(opts cfg.Options) (err error) {
	db, err := connectAndPing(opts)
	if err != nil {
		return serr.Wrap(err)
	}

	// Create a table to store the report
	statsTbl := report.CreateStatsDBTable()

	columnarOn := false
	if opts.DBType == cfg.AlloyDBtype {
		columnarOn, err = IsColumnarEngineOn(db)
	}

	queryDescr := opts.QueryDescr
	query, err := getQueryFromFile(opts.QueryPath)
	if err != nil {
		return serr.Wrap(err)
	}

	// Turn on columnar engine etc, for AlloyDB
	if opts.DBType == cfg.AlloyDBtype {
		// Let's do system alters from psql for now
		// err = alterSystemForColumnar(db)

		err = preSetupColumnar(db)
		if err != nil {
			return serr.Wrap(err)
		}

		// First Batch
		err = RunQueryLoop(opts.DBType, columnarOn, query, db, queryDescr, statsTbl, opts.NbrOfRuns)
		if err != nil {
			return serr.Wrap(err)
		}

		// ---------------------------
		// Call Auto Recommend
		if err = callRecommend(db); err != nil {
			return serr.Wrap(err, "error calling auto columnarization")
		}
		// ---------------------------

		// Run some more
		err = RunQueryLoop(opts.DBType, columnarOn, query, db, queryDescr, statsTbl, opts.NbrOfRuns)
		if err != nil {
			return serr.Wrap(err)
		}

		err = printColumnarStats(db)
		if err != nil {
			return serr.Wrap(err, "error getting columnar stats")
		}

	} else { // Just run the loop for Postgres
		err = RunQueryLoop(opts.DBType, columnarOn, query, db, queryDescr, statsTbl, opts.NbrOfRuns)
		if err != nil {
			return serr.Wrap(err)
		}
	}

	// Explain how we would run the query now
	err = dbquery.ExecQueryWithPrint(db, explainPrefix+query)
	if err != nil {
		return serr.Wrap(err, "error explaining the query")
	}

	// Dump Results
	statsTbl.PrettyPrint(dbtable.PrintOpts{Limit: 50})
	err = statsTbl.QuickCSV("results/" + opts.DBType + "_results.csv")
	if err != nil {
		return serr.Wrap(err, "Unable to output stats CSV for "+opts.DBType)
	}

	return
}

func getQueryFromFile(queryPath string) (query string, err error) {
	qryBytes, err := os.ReadFile(queryPath)
	if err != nil {
		return query, serr.Wrap(err, "Unable to read query from file", "file", queryPath)
	}
	return string(qryBytes), nil
}
