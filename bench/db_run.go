package bench

import (
	"dbx/cfg"
	"dbx/dbase"
	"dbx/queryops"
	"github.com/rohanthewiz/logger"
	"github.com/rohanthewiz/serr"
)

func ExerciseDB(opts cfg.Options) (err error) {
	port := "5432"
	if opts.DBType == cfg.AlloyDBtype {
		port = "5434"
	}

	columnarRequested := opts.DBType == cfg.AlloyDBtype && opts.UseColumnar

	db, err := connectAndPing(opts.DBType, port, err)
	if err != nil {
		return serr.Wrap(err)
	}

	version, err := dbase.GetDBVersion(db)
	logger.Info("DB: " + version)

	statsTbl := queryops.CreateStatsDBTable()

	columnarOn := false
	if opts.DBType == cfg.AlloyDBtype {
		columnarOn, err = IsColumnarEngineOn(db)
	}

	query := SimpleGroupByQuery
	queryDescr := "Simple GroupBy Query"

	if columnarRequested {
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

		err = RunQueryLoop(opts.DBType, columnarOn, query, db, queryDescr, statsTbl, 12)
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
		err = RunQueryLoop(opts.DBType, columnarOn, query, db, queryDescr, statsTbl, 12)
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
