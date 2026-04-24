package postgresql

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/lib/pq"
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
// (” is an escaped quote inside a string). Returns whether the line ends a
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
// transaction on the provided dedicated connection. Consecutive INSERT
// statements targeting the same table are grouped and imported via
// PostgreSQL COPY for better performance. If COPY fails for a group,
// the function falls back to individual INSERT execution.
func (db *DB) importBatchOnConn(ctx context.Context, conn *sql.Conn, stmts []string) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// Defer all constraint checks to the end of this transaction.
	if _, err := tx.ExecContext(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		db.log.Debugf("Could not defer constraints (this is okay if none are deferrable): %v", err)
	}

	var group *insertGroup

	flushGroup := func() error {
		if group == nil {
			return nil
		}
		g := group
		group = nil

		// Try COPY within a savepoint so a failure doesn't abort the tx.
		if _, err := tx.ExecContext(ctx, "SAVEPOINT copy_sp"); err != nil {
			return fmt.Errorf("failed to create savepoint: %v", err)
		}

		if copyErr := db.copyRowsTx(ctx, tx, g.table, g.columns, g.rows); copyErr == nil {
			if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT copy_sp"); err != nil {
				return fmt.Errorf("failed to release savepoint: %v", err)
			}
			db.log.Debugf("COPY %d rows into %s", len(g.rows), g.table)
			return nil
		} else {
			db.log.Debugf("COPY into %s failed (%v), falling back to INSERT", g.table, copyErr)
		}

		if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT copy_sp"); err != nil {
			return fmt.Errorf("failed to rollback savepoint: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT copy_sp"); err != nil {
			return fmt.Errorf("failed to release savepoint: %v", err)
		}

		// Fallback: execute each INSERT individually with savepoints.
		for _, row := range g.rows {
			if err := db.execSingleInsert(ctx, tx, row.raw); err != nil {
				tx.Rollback()
				return err
			}
		}
		return nil
	}

	for _, stmt := range stmts {
		table, columns, valuesStr, isInsert := parseInsert(stmt)
		if !isInsert {
			if err := flushGroup(); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				tx.Rollback()
				return fmt.Errorf("%v %v", err.Error(), stmt)
			}
			continue
		}

		tokens, tokErr := tokenizeValues(valuesStr)
		var goValues []interface{}
		parsedOK := tokErr == nil
		if parsedOK {
			goValues = make([]interface{}, len(tokens))
			for i, tok := range tokens {
				v, ok := sqlValueToGo(tok)
				if !ok {
					parsedOK = false
					break
				}
				goValues[i] = v
			}
		}

		if !parsedOK {
			// Can't parse for COPY — flush group and exec as regular INSERT.
			if err := flushGroup(); err != nil {
				return err
			}
			if err := db.execSingleInsert(ctx, tx, stmt); err != nil {
				tx.Rollback()
				return err
			}
			continue
		}

		// Extend the current group or start a new one.
		if group != nil && group.table == table && columnsMatch(group.columns, columns) {
			group.rows = append(group.rows, pendingRow{values: goValues, raw: stmt})
			continue
		}

		if err := flushGroup(); err != nil {
			return err
		}
		group = &insertGroup{
			table:   table,
			columns: columns,
			rows:    []pendingRow{{values: goValues, raw: stmt}},
		}
	}

	if err := flushGroup(); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// --- COPY helpers ---

type pendingRow struct {
	values []interface{}
	raw    string // original INSERT statement for fallback
}

type insertGroup struct {
	table   string
	columns []string
	rows    []pendingRow
}

func columnsMatch(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// copyRowsTx bulk-inserts rows into table using the PostgreSQL COPY protocol.
func (db *DB) copyRowsTx(ctx context.Context, tx *sql.Tx, table string, columns []string, rows []pendingRow) error {
	stmt, err := tx.PrepareContext(ctx, pq.CopyIn(table, columns...))
	if err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row.values...); err != nil {
			stmt.Close()
			return err
		}
	}
	if _, err := stmt.ExecContext(ctx); err != nil { // flush
		stmt.Close()
		return err
	}
	return stmt.Close()
}

// execSingleInsert executes a single INSERT statement within a savepoint,
// handling duplicate-key and type-mismatch errors gracefully.
func (db *DB) execSingleInsert(ctx context.Context, tx *sql.Tx, stmt string) error {
	if _, err := tx.ExecContext(ctx, "SAVEPOINT row_sp"); err != nil {
		return fmt.Errorf("failed to create savepoint: %v", err)
	}

	if _, execErr := tx.ExecContext(ctx, stmt); execErr != nil {
		tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT row_sp")

		if strings.Contains(execErr.Error(), "duplicate key") {
			db.log.Warnf("duplicate key: %s", execErr)
			tx.ExecContext(ctx, "RELEASE SAVEPOINT row_sp")
			return nil
		}

		if strings.Contains(execErr.Error(), "is of type bytes but expression is of type text") {
			db.log.Debugf("Failed to import because of type issue (%v). Trying to fix...\n", execErr.Error())
			fixed := strings.Replace(
				strings.Replace(stmt, `,convert_from('\x`, ",decode('", 1),
				"'utf-8'", "'hex'", 1)
			if _, fixErr := tx.ExecContext(ctx, fixed); fixErr != nil {
				tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT row_sp")
				tx.ExecContext(ctx, "RELEASE SAVEPOINT row_sp")
				return fmt.Errorf("%v %v", fixErr.Error(), fixed)
			}
			tx.ExecContext(ctx, "RELEASE SAVEPOINT row_sp")
			return nil
		}

		tx.ExecContext(ctx, "RELEASE SAVEPOINT row_sp")
		return fmt.Errorf("%v %v", execErr.Error(), stmt)
	}

	tx.ExecContext(ctx, "RELEASE SAVEPOINT row_sp")
	return nil
}

// --- INSERT statement parsing ---

// parseInsert extracts the table name, column names, and VALUES portion from
// a sanitized INSERT statement (no trailing semicolon).
// Expected format: INSERT INTO "table" ("col1", "col2") VALUES(...)
func parseInsert(stmt string) (table string, columns []string, valuesStr string, ok bool) {
	const prefix = `INSERT INTO "`
	if !strings.HasPrefix(stmt, prefix) {
		return
	}
	rest := stmt[len(prefix):]
	idx := strings.IndexByte(rest, '"')
	if idx < 0 {
		return
	}
	table = rest[:idx]
	rest = strings.TrimLeft(rest[idx+1:], " ")

	// Expect (columns)
	if len(rest) == 0 || rest[0] != '(' {
		return
	}
	closeParen := -1
	inDQ := false
	for i := 1; i < len(rest); i++ {
		if rest[i] == '"' {
			inDQ = !inDQ
		} else if rest[i] == ')' && !inDQ {
			closeParen = i
			break
		}
	}
	if closeParen < 0 {
		return
	}
	colStr := rest[1:closeParen]
	rest = strings.TrimLeft(rest[closeParen+1:], " ")

	for _, c := range strings.Split(colStr, ",") {
		c = strings.TrimSpace(c)
		c = strings.Trim(c, `"`)
		if c != "" {
			columns = append(columns, c)
		}
	}

	if !strings.HasPrefix(rest, "VALUES") {
		return
	}
	valuesStr = strings.TrimLeft(rest[6:], " ")
	ok = true
	return
}

// tokenizeValues splits a "(v1,v2,...)" VALUES list into individual value
// tokens. Handles single-quoted strings with ” escapes and nested
// parentheses (e.g. chr(10)).
func tokenizeValues(s string) ([]string, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return nil, fmt.Errorf("values not parenthesized: %.40s", s)
	}
	inner := s[1 : len(s)-1]

	var tokens []string
	var cur strings.Builder
	inQuote := false
	depth := 0

	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if inQuote {
			cur.WriteByte(ch)
			if ch == '\'' {
				if i+1 < len(inner) && inner[i+1] == '\'' {
					cur.WriteByte('\'')
					i++
				} else {
					inQuote = false
				}
			}
		} else {
			switch ch {
			case '\'':
				inQuote = true
				cur.WriteByte(ch)
			case '(':
				depth++
				cur.WriteByte(ch)
			case ')':
				depth--
				cur.WriteByte(ch)
			case ',':
				if depth == 0 {
					tokens = append(tokens, strings.TrimSpace(cur.String()))
					cur.Reset()
				} else {
					cur.WriteByte(ch)
				}
			default:
				cur.WriteByte(ch)
			}
		}
	}

	last := strings.TrimSpace(cur.String())
	if last != "" || len(tokens) > 0 {
		tokens = append(tokens, last)
	}
	return tokens, nil
}

// sqlValueToGo converts a SQL literal token to a Go value for pq.CopyIn.
// Returns (value, true) on success or (nil, false) if the token can't be evaluated.
func sqlValueToGo(token string) (interface{}, bool) {
	if strings.ToUpper(token) == "NULL" {
		return nil, true
	}

	// Single-quoted string literal
	if len(token) >= 2 && token[0] == '\'' && token[len(token)-1] == '\'' {
		inner := token[1 : len(token)-1]
		// Hex-encoded bytea: '\xABCD'
		if strings.HasPrefix(inner, `\x`) || strings.HasPrefix(inner, `\X`) {
			b, err := hex.DecodeString(inner[2:])
			if err != nil {
				return nil, false
			}
			return b, true
		}
		return strings.ReplaceAll(inner, "''", "'"), true
	}

	// String concatenation: 'a'||chr(10)||'b'
	if strings.Contains(token, "||") {
		val, err := evalConcat(token)
		if err != nil {
			return nil, false
		}
		return val, true
	}

	// Standalone chr(N)
	lower := strings.ToLower(token)
	if strings.HasPrefix(lower, "chr(") && strings.HasSuffix(token, ")") {
		val, err := evalChr(token)
		if err != nil {
			return nil, false
		}
		return val, true
	}

	// Numeric or other literal — pass as string; Postgres casts during COPY.
	return token, true
}

// evalChr evaluates "chr(N)" to its UTF-8 character string.
func evalChr(s string) (string, error) {
	inner := s[4 : len(s)-1]
	n, err := strconv.Atoi(strings.TrimSpace(inner))
	if err != nil {
		return "", err
	}
	return string(rune(n)), nil
}

// evalConcat evaluates a SQL || concatenation expression containing string
// literals and chr(N) calls.
func evalConcat(s string) (string, error) {
	parts := splitConcat(s)
	var b strings.Builder
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if len(p) >= 2 && p[0] == '\'' && p[len(p)-1] == '\'' {
			inner := p[1 : len(p)-1]
			b.WriteString(strings.ReplaceAll(inner, "''", "'"))
		} else if strings.HasPrefix(strings.ToLower(p), "chr(") && strings.HasSuffix(p, ")") {
			ch, err := evalChr(p)
			if err != nil {
				return "", err
			}
			b.WriteString(ch)
		} else {
			return "", fmt.Errorf("unsupported concat operand: %s", p)
		}
	}
	return b.String(), nil
}

// splitConcat splits a SQL expression on || while respecting single-quoted strings.
func splitConcat(s string) []string {
	var parts []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		if inQuote {
			cur.WriteByte(s[i])
			if s[i] == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					cur.WriteByte('\'')
					i++
				} else {
					inQuote = false
				}
			}
		} else if s[i] == '\'' {
			inQuote = true
			cur.WriteByte(s[i])
		} else if i+1 < len(s) && s[i] == '|' && s[i+1] == '|' {
			parts = append(parts, strings.TrimSpace(cur.String()))
			cur.Reset()
			i++ // skip second |
		} else {
			cur.WriteByte(s[i])
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, strings.TrimSpace(cur.String()))
	}
	return parts
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
