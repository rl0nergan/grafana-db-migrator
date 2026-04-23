package sqlite

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

// Package-level compiled regexes (compiled once at startup).
var (
	reBacktick       = regexp.MustCompile("`")
	rePragmaBeginSeq = regexp.MustCompile(`(?m)[\r\n]?^(PRAGMA.*;|BEGIN.*;|.*sqlite_sequence.*;)$`)
	reInsertQuote    = regexp.MustCompile(`(?msU)^(INSERT INTO) "?([a-zA-Z0-9_]*)"? (VALUES.*;)$`)
	reRemoveCreate   = regexp.MustCompile(`(?msU)[\r\n]+^CREATE.*;$`)
	reHexDecode      = regexp.MustCompile(`X'([a-fA-F0-9]+)'`)
	reInsertCols     = regexp.MustCompile(`(?m)^(INSERT INTO "([^"]+)") (VALUES.*)$`)
	reCharToChr      = regexp.MustCompile(`\bchar\s*\(`)
)

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

// SanitizePipeline reads the SQLite dump at inPath, applies all sanitization
// transforms in a single streaming pass, and writes the result to outPath.
// columnMap must be pre-fetched from the SQLite file via GetTableColumns.
func SanitizePipeline(inPath, outPath string, columnMap map[string][]string) error {
	in, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	bw := bufio.NewWriterSize(out, 1<<20) // 1 MB write buffer
	if err := sanitizePipelineStream(in, bw, columnMap); err != nil {
		return err
	}
	return bw.Flush()
}

// sanitizePipelineStream is the testable core of SanitizePipeline.
// It reads from r line-by-line and writes all transformed lines to w.
func sanitizePipelineStream(r io.Reader, w io.Writer, columnMap map[string][]string) error {
	// Pre-build quoted column list strings per table so we don't allocate per-row.
	colListCache := make(map[string]string, len(columnMap))
	for table, cols := range columnMap {
		colListCache[table] = buildColList(cols)
	}

	br := bufio.NewReaderSize(r, 1<<20) // 1 MB read buffer
	inCreateBlock := false
	inSkipBlock := false // true when mid-way through skipping a multi-line statement
	inString := false    // quote parity tracker across lines

	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			s := strings.TrimRight(string(line), "\r\n")

			// Quote-aware statement boundary detection — must run for every
			// line to keep inString accurate for subsequent lines.
			endsStmt, newInString := advanceQuoteState(s, inString)

			// --- Skip continuation: mid-way through skipping a multi-line statement ---
			if inSkipBlock {
				inString = newInString
				if endsStmt {
					inSkipBlock = false
				}
				goto next
			}

			// --- CREATE block state machine ---
			// CREATE DDL in the dump doesn't have string values with embedded ;\n,
			// so a simple suffix check is sufficient to detect statement end.
			if inCreateBlock {
				inString = newInString
				if strings.HasSuffix(s, ";") {
					inCreateBlock = false
				}
				goto next
			}
			if strings.HasPrefix(s, "CREATE") {
				inString = newInString
				if !strings.HasSuffix(s, ";") {
					inCreateBlock = true
				}
				goto next
			}

			// --- Line-level skip rules ---
			{
				shouldSkip := s == "" ||
					(strings.HasPrefix(s, "PRAGMA") && strings.HasSuffix(s, ";")) ||
					(strings.HasPrefix(s, "BEGIN") && strings.HasSuffix(s, ";")) ||
					strings.Contains(s, "sqlite_sequence") ||
					strings.HasPrefix(s, `INSERT INTO "migration_log"`) ||
					strings.HasPrefix(s, `INSERT INTO "_litestream_seq"`)

				if shouldSkip {
					inString = newInString
					// If this line doesn't end the statement, subsequent lines
					// are continuations that also need to be skipped.
					if s != "" && !endsStmt {
						inSkipBlock = true
					}
					goto next
				}
			}

			// --- Transforms ---

			// 1. Backtick → double-quote
			if strings.ContainsRune(s, '`') {
				s = strings.ReplaceAll(s, "`", `"`)
			}

			// 2. char( → chr(
			if strings.Contains(s, "char") {
				s = reCharToChr.ReplaceAllString(s, "chr(")
			}

			// 3. X'hexval' → '\xhexval'
			if strings.Contains(s, "X'") {
				s = reHexDecode.ReplaceAllString(s, `'\x$1'`)
			}

			// 4. Ensure INSERT INTO table name is quoted
			if strings.HasPrefix(s, "INSERT INTO") && !strings.HasPrefix(s, `INSERT INTO "`) {
				s = reInsertQuote.ReplaceAllString(s, `$1 "$2" $3`)
			}

			// 5. Inject column names into INSERT INTO statements
			if strings.HasPrefix(s, "INSERT INTO") {
				if m := reInsertCols.FindStringSubmatch(s); m != nil {
					tableName := m[2]
					if colList, ok := colListCache[tableName]; ok {
						s = fmt.Sprintf(`%s (%s) %s`, m[1], colList, m[3])
					}
				}
			}

			if _, werr := fmt.Fprintln(w, s); werr != nil {
				return werr
			}
			inString = newInString
		}

	next:
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// buildColList returns a quoted, comma-joined column list string for use in
// INSERT INTO "table" (colList) VALUES ... statements.
func buildColList(cols []string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = `"` + c + `"`
	}
	return strings.Join(quoted, ", ")
}

// Sanitize cleans up a SQLite dump file to prep it for import into Postgres.
func Sanitize(dumpFile string) error {
	data, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}
	sanitized := reBacktick.ReplaceAll(data, []byte(`"`))
	sanitized = rePragmaBeginSeq.ReplaceAll(sanitized, nil)
	sanitized = reInsertQuote.ReplaceAll(sanitized, []byte(`$1 "$2" $3`))
	return ioutil.WriteFile(dumpFile, sanitized, 0644)
}

// CustomSanitize allows you to expand upon the default Sanitize function
// by providing your own regex matcher and replacement to modify data from the dump file.
func CustomSanitize(dumpFile string, regex string, replacement []byte) error {
	re := regexp.MustCompile(regex)
	data, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}
	sanitized := re.ReplaceAll(data, replacement)
	return ioutil.WriteFile(dumpFile, sanitized, 0644)
}

// RemoveCreateStatements takes all the CREATE statements out of a dump
// so that no new tables are created.
func RemoveCreateStatements(dumpFile string) error {
	data, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}
	sanitized := reRemoveCreate.ReplaceAll(data, nil)
	return ioutil.WriteFile(dumpFile, sanitized, 0644)
}

// HexDecode takes a file path containing a SQLite dump and
// decodes any hex-encoded data it finds.
func HexDecode(dumpFile string) error {
	data, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}
	sanitized := reHexDecode.ReplaceAll(data, []byte(`'\x$1'`))
	return ioutil.WriteFile(dumpFile, sanitized, 0644)
}

// AddColumnNames adds explicit column names to INSERT statements in the dump file
// so that values are mapped correctly regardless of column order in the target database.
func AddColumnNames(dumpFile string, columns map[string][]string) error {
	data, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}

	colListCache := make(map[string]string, len(columns))
	for table, cols := range columns {
		colListCache[table] = buildColList(cols)
	}

	sanitized := reInsertCols.ReplaceAllFunc(data, func(match []byte) []byte {
		m := reInsertCols.FindSubmatch(match)
		tableName := string(m[2])
		colList, ok := colListCache[tableName]
		if !ok {
			return match
		}
		return []byte(fmt.Sprintf(`%s (%s) %s`, string(m[1]), colList, string(m[3])))
	})

	return ioutil.WriteFile(dumpFile, sanitized, 0644)
}
