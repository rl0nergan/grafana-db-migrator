package postgresql

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	// Postgres driver
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

// DB allows for interface methods.
// It just holds a connection pointer.
type DB struct {
	conn *sql.DB
	log  *logrus.Logger
}

// New returns a Postgres database connection.
func New(connString string, logger *logrus.Logger) (db DB, err error) {
	db.log = logger
	db.conn, err = sql.Open("postgres", connString)
	if err != nil {
		return
	}
	_, err = db.conn.Exec("SELECT 1")
	return
}

// ImportDump imports a SQL dump file. If batchSize is 0, all statements are
// executed in a single transaction. Otherwise statements are split into
// transactions of at most batchSize statements each.
func (db *DB) ImportDump(dumpFile string, batchSize int) error {

	promptToContinue := func() bool {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("You seem to have encountered some errors. Would you still like to continue? [Y/n]: ")
		text, _ := reader.ReadString('\n')
		switch response := strings.ToLower(text); response {
		case "n\n":
			return false
		default:
			return true
		}
	}

	// Alter tables because of boolean issues
	// SQLite has booleans as 1's and 0's
	// Postgres is true/false
	// We'll convert it after importing the dump.
	if errorEncountered := db.prepareTables(); errorEncountered == true {
		if promptToContinue() != true {
			return fmt.Errorf("%s", "Stopping migration at user's request.")
		}
	}

	f, err := os.Open(dumpFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Acquire a dedicated connection so session settings persist across all
	// batch transactions without re-issuing them per batch.
	ctx := context.Background()
	conn, err := db.conn.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %v", err)
	}
	defer func() {
		conn.ExecContext(ctx, "SET session_replication_role = 'origin'")
		conn.Close()
	}()

	// Set once for the entire import; persists across transactions on this connection.
	if _, err := conn.ExecContext(ctx, "SET session_replication_role = 'replica'"); err != nil {
		db.log.Debugf("Could not set session_replication_role (ok if not superuser): %v", err)
	}

	batchNum := 0
	if err := streamStatements(f, batchSize, func(batch []string) error {
		batchNum++
		db.log.Infof("Importing batch %d (%d statements)", batchNum, len(batch))
		return db.importBatchOnConn(ctx, conn, batch)
	}); err != nil {
		return fmt.Errorf("batch %d failed: %v", batchNum, err)
	}

	// Fix boolean columns that we converted before.
	if errorEncountered := db.decodeBooleanColumns(); errorEncountered == true {
		if promptToContinue() != true {
			return fmt.Errorf("%s", "Stopping migration at user's request.")
		}
	}

	// Fix sequences for new items.
	if err := db.fixSequences(); err != nil {
		return err
	}

	return nil
}

// advanceQuoteState processes s tracking single-quoted SQL string state
// ('' is an escaped quote inside a string). Returns whether the line ends a
// SQL statement and the updated in-string state.
func advanceQuoteState(s string, inString bool) (endsStatement bool, newInString bool) {
	for i := 0; i < len(s); i++ {
		if inString {
			if s[i] == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++ // skip escaped ''
				} else {
					inString = false
				}
			}
		} else {
			if s[i] == '\'' {
				inString = true
			}
		}
	}
	return !inString && len(s) > 0 && s[len(s)-1] == ';', inString
}

// streamStatements reads SQL statements from r and invokes fn for each
// complete batch. Statements may span multiple lines (e.g. when string values
// contain embedded newlines); quote-aware parsing detects statement boundaries.
// COMMIT statements from the sqlite3 dump are discarded — each batch manages
// its own transaction.
func streamStatements(r io.Reader, batchSize int, fn func([]string) error) error {
	br := bufio.NewReaderSize(r, 1<<20) // 1 MB read buffer
	var batch []string
	var stmtLines []string
	inString := false

	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			s := strings.TrimRight(string(line), "\r\n")

			if s == "" {
				goto checkErr
			}

			// Skip bare COMMIT lines — batches handle their own commits.
			if s == "COMMIT" || s == "COMMIT;" {
				goto checkErr
			}

			{
				endsStmt, newInString := advanceQuoteState(s, inString)
				inString = newInString
				stmtLines = append(stmtLines, s)

				if endsStmt {
					var stmt string
					if len(stmtLines) == 1 {
						stmt = stmtLines[0]
					} else {
						stmt = strings.Join(stmtLines, "\n")
					}
					stmt = strings.TrimSpace(stmt)
					if strings.HasSuffix(stmt, ";") {
						stmt = stmt[:len(stmt)-1]
					}
					if stmt != "" {
						batch = append(batch, stmt)
						if batchSize > 0 && len(batch) >= batchSize {
							if err2 := fn(batch); err2 != nil {
								return err2
							}
							batch = batch[:0] // reset, reuse backing array
						}
					}
					stmtLines = stmtLines[:0]
				}
			}
		}

	checkErr:
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	if len(batch) > 0 {
		return fn(batch)
	}
	return nil
}

// importBatchOnConn executes a slice of SQL statements inside a single
// transaction on the provided dedicated connection.
func (db *DB) importBatchOnConn(ctx context.Context, conn *sql.Conn, stmts []string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// Defer all constraint checks to the end of this transaction.
	if _, err := tx.ExecContext(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		db.log.Debugf("Could not defer constraints (this is okay if none are deferrable): %v", err)
	}

	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			// We can safely ignore "duplicate key value violates unique constraint" errors.
			if strings.Contains(err.Error(), "duplicate key") {
				db.log.Warnf("duplicate key: %s", err)
				// Rollback and start a new transaction; session settings persist on the connection.
				tx.Rollback()
				tx, err = conn.BeginTx(ctx, nil)
				if err != nil {
					return fmt.Errorf("failed to begin transaction: %v", err)
				}
				continue
			} else if strings.Contains(err.Error(), "is of type bytes but expression is of type text") {
				db.log.Debugf("Failed to import because of type issue (%v). Trying to fix...\n", err.Error())
				tx.Rollback()
				tx, err = conn.BeginTx(ctx, nil)
				if err != nil {
					return fmt.Errorf("failed to begin transaction: %v", err)
				}
				stmt = strings.Replace(
					strings.Replace(stmt, `,convert_from('\x`, ",decode('", 1),
					"'utf-8'", "'hex'", 1)
				if _, err := tx.ExecContext(ctx, stmt); err != nil {
					tx.Rollback()
					return fmt.Errorf("%v %v", err.Error(), stmt)
				}
			} else {
				tx.Rollback()
				return fmt.Errorf("%v %v", err.Error(), stmt)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

const maxFixupConcurrency = 4

// Change column types that expect boolean to integer so that we can get the data in.
// We'll decode their values into booleans later.
func (db *DB) prepareTables() (errorEncountered bool) {
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		sem    = make(chan struct{}, maxFixupConcurrency)
		errOut bool
	)

	for _, table := range TableChanges {
		table := table
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			for _, column := range table.Columns {
				if column.Default != "" {
					stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", table.Table, column.Name)
					db.log.Debugln("Executing: ", stmt)
					if _, err := db.conn.Exec(stmt); err != nil {
						if strings.Contains(err.Error(), "does not exist") {
							db.log.Debugf("%s %v %v", "Column/table doesn't exist. This is usually fine to ignore, but here's the info:", err.Error(), stmt)
						} else {
							db.log.Warnf("%v %v", err.Error(), stmt)
							mu.Lock()
							errOut = true
							mu.Unlock()
						}
					}
				}

				stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE integer USING %s::integer", table.Table, column.Name, column.Name)
				db.log.Debugln("Executing: ", stmt)
				if _, err := db.conn.Exec(stmt); err != nil {
					if strings.Contains(err.Error(), "does not exist") {
						db.log.Debugf("%s %v %v", "Column/table doesn't exist. This is usually fine to ignore, but here's the info:", err.Error(), stmt)
					} else {
						db.log.Warnf("%v %v", err.Error(), stmt)
						mu.Lock()
						errOut = true
						mu.Unlock()
					}
				}
			}
		}()
	}

	wg.Wait()

	// Delete the org that gets auto-generated the first time Grafana runs.
	// Must run after all ALTERs complete.
	stmt := "DELETE FROM org WHERE id=1"
	db.log.Debugln("Executing: ", stmt)
	if _, err := db.conn.Exec(stmt); err != nil {
		db.log.Errorf("%v %v", err.Error(), stmt)
		errOut = true
	}

	return errOut
}

// Change columns back to boolean type by decoding their current values.
func (db *DB) decodeBooleanColumns() bool {
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		sem    = make(chan struct{}, maxFixupConcurrency)
		errOut bool
	)

	for _, table := range TableChanges {
		table := table
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			for _, column := range table.Columns {
				stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE boolean USING CASE WHEN %s = 0 THEN FALSE WHEN %s = 1 THEN TRUE ELSE NULL END", table.Table, column.Name, column.Name, column.Name)
				db.log.Debugln("Executing: ", stmt)
				if _, err := db.conn.Exec(stmt); err != nil {
					if strings.Contains(err.Error(), "does not exist") {
						db.log.Debugf("%s %v %v", "Column/table doesn't exist. This is usually fine to ignore, but here's the info:", err.Error(), stmt)
					} else {
						db.log.Warnf("%v %v", err.Error(), stmt)
						mu.Lock()
						errOut = true
						mu.Unlock()
					}
				}

				if column.Default != "" {
					stmt = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", table.Table, column.Name, column.Default)
					db.log.Debugln("Executing: ", stmt)
					if _, err := db.conn.Exec(stmt); err != nil {
						if strings.Contains(err.Error(), "does not exist") {
							db.log.Debugf("%s %v %v", "Column/table doesn't exist. This is usually fine to ignore, but here's the info:", err.Error(), stmt)
						} else {
							db.log.Warnf("%v %v", err.Error(), stmt)
							mu.Lock()
							errOut = true
							mu.Unlock()
						}
					}
				}
			}
		}()
	}

	wg.Wait()
	return errOut
}

// Make sure that sequences are fine on the tables.
func (db *DB) fixSequences() error {

	// Query from https://wiki.postgresql.org/wiki/Fixing_Sequences
	stmt := `SELECT 'SELECT SETVAL(' ||
	quote_literal(quote_ident(PGT.schemaname) || '.' || quote_ident(S.relname)) ||
	', COALESCE(MAX(' ||quote_ident(C.attname)|| '), 1) ) FROM ' ||
	quote_ident(PGT.schemaname)|| '.'||quote_ident(T.relname)|| ';' stmt
FROM pg_class AS S,
pg_depend AS D,
pg_class AS T,
pg_attribute AS C,
pg_tables AS PGT
WHERE S.relkind = 'S'
AND S.oid = D.objid
AND D.refobjid = T.oid
AND D.refobjid = C.attrelid
AND D.refobjsubid = C.attnum
AND T.relname = PGT.tablename
ORDER BY S.relname;`

	db.log.Debugln("Running query to generate statements to reset all sequences.")
	rows, err := db.conn.Query(stmt)
	if err != nil {
		return fmt.Errorf("%v %v", err.Error(), stmt)
	}
	defer rows.Close()

	db.log.Debugln("Running generated queries to reset all sequences.")
	for rows.Next() {
		var stmt string
		if err := rows.Scan(&stmt); err != nil {
			return fmt.Errorf("%v %v", "Failed to retrieve sequence reset statement", err)
		}

		// Execute the generated statement
		if _, err := db.conn.Exec(stmt); err != nil {
			return fmt.Errorf("%v %v", err.Error(), stmt)
		}
	}

	return nil
}
