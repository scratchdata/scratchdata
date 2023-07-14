package main

import (
	"database/sql"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/spyzhov/ajson"
)

type DuckDBStorage struct {
	filename string
	db       *sql.DB
}

func CreateDuckDBStorage(filename string) (*DuckDBStorage, error) {
	s := &DuckDBStorage{
		filename: filename,
	}

	cores := runtime.NumCPU()
	threads := cores - 1
	if threads == 0 {
		threads = 1
	}
	// workers := threads / 2

	db_url := fmt.Sprintf("%s?threads=%d&memory_limit=1GB", filename, threads)

	connector, err := duckdb.NewConnector(db_url, nil)
	if err != nil {
		return nil, err
	}

	s.db = sql.OpenDB(connector)

	return s, nil
}

func (s *DuckDBStorage) Query(query string) (*sql.Rows, error) {
	rows, err := s.db.Query(query)
	return rows, err
}

func (s *DuckDBStorage) Close() error {
	return s.db.Close()
}

func (s *DuckDBStorage) WriteJSONRow(table string, root []*ajson.Node) error {

	// TODO: validate+escape table name
	_, err := s.db.Exec("create table if not exists " + table + " (__row_id UBIGINT)")
	if err != nil {
		return err
	}

	// TODO: validate+escape sequence name
	_, err = s.db.Exec("create sequence if not exists " + table + " start 1")
	if err != nil {
		return err
	}

	nodes := root
	// nodes, err := root.JSONPath("$.data")
	// if err != nil {
	// return err
	// }

	columns := make([]string, 0)

	for _, node := range nodes { // TODO: we shoudl not need this loop

		// Create additional columns in table if needed
		// TODO: support non-string types
		for _, key := range node.Keys() {

			// todo: validate+escape column name (any special character except underscore)
			c := "\"" + strings.TrimSpace(strings.ToLower(key)) + "\""
			columns = append(columns, c)
			sql := "alter table " + table + " add column if not exists " + c + " varchar"
			_, err = s.db.Exec(sql)
			if err != nil {
				return err
			}
		}

		// Create parameteried SQL insert
		sql := "insert into " + table + " (__row_id,"
		sql += strings.Join(columns, ",")
		sql += ") values (nextval('" + table + "'), "
		sql += strings.Join(strings.Split(strings.Repeat("?", len(columns)), ""), ",")
		sql += ")"

		// Prepare JSON values for insert
		vals := make([]interface{}, len(columns))
		for i, c := range node.Keys() {
			v, err := node.GetKey(c)
			if err != nil {
				return err
			}
			if v.IsString() {
				vals[i], err = strconv.Unquote(v.String())
				if err != nil {
					return err
				}
			} else {
				vals[i] = v.String()
			}

		}

		// Insert data
		_, err := s.db.Exec(sql, vals...)
		if err != nil {
			return err
		}
	}

	return nil
}
