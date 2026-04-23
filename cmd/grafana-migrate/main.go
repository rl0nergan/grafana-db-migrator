package main

import (
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/percona/grafana-db-migrator/pkg/postgresql"
	"github.com/percona/grafana-db-migrator/pkg/sqlite"

	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	log                = logrus.New()
	app                = kingpin.New("Grafana SQLite to Postgres Migrator", "A command-line application to migrate Grafana data from SQLite to Postgres.")
	dump               = app.Flag("dump", "Directory path where the sqlite dump should be stored.").Default("/tmp").ExistingDir()
	sqlitefile         = app.Arg("sqlite-file", "Path to SQLite file being imported.").Required().File()
	connstring         = app.Arg("postgres-connection-string", "URL-format database connection string to use in the URL format (postgres://USERNAME:PASSWORD@HOST/DATABASE).").Required().String()
	debug              = app.Flag("debug", "Enable debug level logging").Bool()
	resetHomeDashboard = app.Flag("reset-home-dashboard", "Reset home dashboard for default organization").Bool()
	changeCharToText   = app.Flag("change-char-to-text", "Change CHAR filed to TEXT").Bool()
	// fix relationshop between dashboard and folders (provisioning error)
	fixFoldersID = app.Flag("fix-folders-id", "Fix correlation between folders and dashboards").Bool()
	pprofFile    = app.Flag("pprof", "Write CPU profile to the given file path").String()
	batchSize    = app.Flag("batch-size", "Number of SQL statements per transaction batch (0 for single transaction)").Default("0").Int()
)

func main() {

	kingpin.MustParse(app.Parse(os.Args[1:]))
	log.SetFormatter(&logrus.TextFormatter{
		DisableLevelTruncation: true,
		FullTimestamp:          true,
	})

	if *debug {
		log.SetLevel(logrus.DebugLevel)
	}

	if *pprofFile != "" {
		f, err := os.Create(*pprofFile)
		if err != nil {
			log.Fatalf("❌ could not create CPU profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("❌ could not start CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigCh
			log.Infof("Interrupted, flushing CPU profile to %s", *pprofFile)
			pprof.StopCPUProfile()
			f.Close()
			os.Exit(1)
		}()

		log.Infof("CPU profiling enabled, writing to %s", *pprofFile)
	}

	dumpPath := *dump + "/grafana.sql"

	// Must dereference
	sqliteF := *sqlitefile
	log.Infof("📁 SQLlite file: %v", sqliteF.Name())
	log.Infof("📁 Dump directory: %v", *dump)

	// Make sure SQLite exists on machine
	if err := sqlite.Exists(); err != nil {
		log.Fatalf("❌ %v - is the sqlite3 command line tool installed?", err)
	}
	log.Infof("✅ sqlite3 command exists")

	if *fixFoldersID {
		db, err := postgresql.New(*connstring, log)
		if err != nil {
			log.Fatalf("❌ %v - failed to connect to Postgres database.", err)
		}
		// Get folder/dashboard relationshio for fixing after upgrade
		sqliteDashboardTree, sqliteFolders, err := sqlite.GetFolders(sqliteF.Name())
		if err != nil {
			log.Fatalf("❌ %v - failed to get relationship between folders and dashboards.", err)
		}
		log.Infoln("✅ got folder/dashboard relationship from SQLite")
		if len(sqliteFolders) != 0 {
			log.Warnf("⚠️ Found %d orphaned folders in SQLite", len(sqliteFolders))
		}

		if err := db.FixFolderID(sqliteDashboardTree); err != nil {
			log.Fatalf("❌ %v - failed to fix folders ID.", err)
		}
		log.Infoln("✅ folders ID was fixed")
		log.Infoln("🎉 All done!")
		os.Exit(0)
	}

	// Dump the SQLite database
	if err := sqlite.Dump(sqliteF.Name(), dumpPath); err != nil {
		log.Fatalf("❌ %v - failed to dump database.", err)
	}
	log.Infof("✅ sqlite3 database dumped to %v", dumpPath)

	// Get column names from SQLite before sanitization so the pipeline can
	// inject them inline (reads the SQLite file directly, not the dump).
	columnMap, err := sqlite.GetTableColumns(sqliteF.Name())
	if err != nil {
		log.Fatalf("❌ %v - failed to get column names from SQLite database.", err)
	}

	// Single-pass sanitization: replaces the previous 8-step read-write cycle.
	sanitizedPath := dumpPath + ".sanitized"
	if err := sqlite.SanitizePipeline(dumpPath, sanitizedPath, columnMap); err != nil {
		log.Fatalf("❌ %v - failed to sanitize dump file.", err)
	}
	log.Infoln("✅ sqlite3 dump sanitized")

	// Connect to Postgres
	db, err := postgresql.New(*connstring, log)
	if err != nil {
		log.Fatalf("❌ %v - failed to connect to Postgres database.", err)
	}

	// Import the now-sanitized dump file into Postgres
	if err := db.ImportDump(sanitizedPath, *batchSize); err != nil {
		log.Fatalf("❌ %v - failed to import dump file to Postgres.", err)
	}
	log.Infoln("✅ Imported dump file to Postgres")

	if err := db.ChangeHEXToText(); err != nil {
		log.Fatalf("❌ %v - failed to change hex values in Postgres.", err)
	}
	log.Infoln("✅ Fixed hex values in Postgres")

	if *resetHomeDashboard {
		if err := db.FixHomeDashboard(); err != nil {
			log.Fatalf("❌ %v - failed to change home dashboard.", err)
		}
		log.Infoln("✅ home dashboard was changed to default.")
	}

	if *changeCharToText {
		if err := db.ChangeCharToText(); err != nil {
			log.Fatalf("❌ %v - failed convert CHAR type to TEXT", err)
		}
		log.Infoln("✅ CHAR type was converted to TEXT.")
	}
	log.Infoln("🎉 All done!")

}
