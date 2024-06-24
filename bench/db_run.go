package bench

import (
	"dbx/cfg"
	"dbx/report"

	"github.com/rohanthewiz/logger"
	"github.com/rohanthewiz/serr"
)

func ExerciseDB(opts cfg.Options) (err error) {
	const myQuery = `your query here` // TODO: Replace with your query
	const nbrOfRuns = 12

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

	query := myQuery
	queryDescr := "My Query Description" // TODO: Replace with your query description

	if opts.DBType == cfg.AlloyDBtype && opts.Columnar {
		if !columnarOn {
			err = alterSystemForColumnar(db)
			if err != nil {
				return serr.Wrap(err, "error trying to turn columnar engine on")
			}
			logger.Warn("Columnar changes require a DB restart",
				"cmd to run", "sudo alloydb database-server stop && sudo alloydb database-server start")
			return serr.New("AlloyDB must now be restarted")
		}

		err = preSetupColumnar(db)
		if err != nil {
			return serr.Wrap(err)
		}

		err = RunQueryLoop(opts.DBType, columnarOn, query, db, queryDescr, statsTbl, nbrOfRuns)
		if err != nil {
			return serr.Wrap(err)
		}

		err = columnarStats(db)
		if err != nil {
			return serr.Wrap(err, "error getting columnar stats")
		}

	} else { // columnar not requested but could be previously on
		if opts.DBType == cfg.AlloyDBtype {
			err = preSetupColumnar(db)
			if err != nil {
				return serr.Wrap(err)
			}
		}

		err = RunQueryLoop(opts.DBType, columnarOn, query, db, queryDescr, statsTbl, nbrOfRuns)
		if err != nil {
			return serr.Wrap(err)
		}
	}

	// Dump Results
	statsTbl.PrettyPrint()
	err = statsTbl.QuickCSV(opts.DBType + "_results.csv")
	if err != nil {
		return serr.Wrap(err, "Unable to output stats CSV for "+opts.DBType)
	}

	return
}
