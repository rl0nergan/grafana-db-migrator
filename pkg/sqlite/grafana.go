package sqlite

import (
	"database/sql"
	"fmt"

	"github.com/percona/grafana-db-migrator/pkg/common"

	_ "modernc.org/sqlite"
)

func GetFolders(dbFile string) (*common.Tree, map[int]*common.Folder, error) {
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return nil, nil, err
	}
	return common.GetTree(db)
}

// GetTableColumns returns a map of table name to ordered column names from the SQLite database.
func GetTableColumns(dbFile string) (map[string][]string, error) {
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}

	columns := make(map[string][]string)
	for _, table := range tables {
		pragmaRows, err := db.Query(fmt.Sprintf(`PRAGMA table_info("%s")`, table))
		if err != nil {
			return nil, err
		}

		var cols []string
		for pragmaRows.Next() {
			var cid int
			var colName, colType string
			var notNull int
			var dfltValue sql.NullString
			var pk int
			if err := pragmaRows.Scan(&cid, &colName, &colType, &notNull, &dfltValue, &pk); err != nil {
				pragmaRows.Close()
				return nil, err
			}
			cols = append(cols, colName)
		}
		pragmaRows.Close()
		columns[table] = cols
	}

	return columns, nil
}
