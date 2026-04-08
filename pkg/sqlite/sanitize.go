package sqlite

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
)

// Sanitize cleans up a SQLite dump file to prep it for import into Postgres.
func Sanitize(dumpFile string) error {
	// Change ` to "
	re := regexp.MustCompile("`")
	data, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}
	sanitized := re.ReplaceAll(data, []byte("\""))

	// Remove SQLite-specific PRAGMA statements
	// and statements that start with BEGIN
	// and statements pertaining to the sqlite_sequence table.
	re = regexp.MustCompile(`(?m)[\r\n]?^(PRAGMA.*;|BEGIN.*;|.*sqlite_sequence.*;)$`)
	sanitized = re.ReplaceAll(sanitized, nil)

	// Ensure there are quotes around table names to avoid using reserved table names like user.
	re = regexp.MustCompile(`(?msU)^(INSERT INTO) "?([a-zA-Z0-9_]*)"? (VALUES.*;)$`)
	sanitized = re.ReplaceAll(sanitized, []byte(`$1 "$2" $3`))

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
	re := regexp.MustCompile(`(?msU)[\r\n]+^CREATE.*;$`)
	data, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}
	sanitized := re.ReplaceAll(data, nil)
	return ioutil.WriteFile(dumpFile, sanitized, 0644)
}

// HexDecode takes a file path containing a SQLite dump and
// decodes any hex-encoded data it finds.
func HexDecode(dumpFile string) error {
	re := regexp.MustCompile(`(?m)X\'([a-fA-F0-9]+)\'`)
	data, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}

	// Define a function to wrap encoded hex data in a call to decode hexstring.
	decodeHex := func(hexEncoded []byte) []byte {
		return []byte(fmt.Sprintf("'%s%s'", `\x`, re.FindSubmatch(hexEncoded)[1]))
	}

	// Replace regex matches from the dumpFile using the `decodeHex` function defined above.
	sanitized := re.ReplaceAllFunc(data, decodeHex)
	return ioutil.WriteFile(dumpFile, sanitized, 0644)
}

// AddColumnNames adds explicit column names to INSERT statements in the dump file
// so that values are mapped correctly regardless of column order in the target database.
func AddColumnNames(dumpFile string, columns map[string][]string) error {
	data, err := ioutil.ReadFile(dumpFile)
	if err != nil {
		return err
	}

	re := regexp.MustCompile(`(?m)^(INSERT INTO "([^"]+)") (VALUES.*)$`)
	sanitized := re.ReplaceAllFunc(data, func(match []byte) []byte {
		submatches := re.FindSubmatch(match)
		tableName := string(submatches[2])
		cols, ok := columns[tableName]
		if !ok {
			return match
		}
		quotedCols := make([]string, len(cols))
		for i, col := range cols {
			quotedCols[i] = fmt.Sprintf(`"%s"`, col)
		}
		colList := strings.Join(quotedCols, ", ")
		return []byte(fmt.Sprintf("%s (%s) %s", string(submatches[1]), colList, string(submatches[3])))
	})

	return ioutil.WriteFile(dumpFile, sanitized, 0644)
}
