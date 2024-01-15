package redshift

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"scratchdata/util"
	"strings"

	"github.com/rs/zerolog/log"
)

var jsonToPgType = map[string]string{
	"string": "text",
	"int":    "bigint",
	"float":  "double precision",
	"bool":   "boolean",
}

func (s *RedshiftServer) createTable(table string) error {
	sql := (&util.StringBuffer{}).
		Printf(`create table if not exists`).
		Space().
		Printf(s.sqlSchemaDatabasePfx).
		SQLIdent(table).
		Space().
		Printf(`(__row_id text)`).
		String()
	_, err := s.db.Exec(sql)
	if err != nil {
		return fmt.Errorf("createTable: %#q: %w", sql, err)
	}
	return nil
}

func (s *RedshiftServer) getGolumnNames(table string) (map[string]bool, error) {
	schema := "public"
	if s.Schema != "" {
		schema = s.Schema
	}

	sql := `
		select "column" as column_name
		from pg_table_def
		where schemaname = $1 and tablename = $2
	`
	if s.DatabaseIsPostgres {
		sql = `
			select column_name
			from information_schema.columns
			where table_schema = $1 and table_name = $2
		`
	}

	m := map[string]bool{}
	rows, err := s.db.Query(sql, schema, table)
	if err != nil {
		return nil, fmt.Errorf("getGolumnNames: cannot fetch column names: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		name := ""
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("getGolumnNames: cannot scan column name: %w", err)
		}
		m[strings.ToLower(name)] = true
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("getGolumnNames: no columns found: %w", err)
	}
	return m, nil
}

func (s *RedshiftServer) createColumns(table string, jsonTypes map[string]string) error {
	// Redshift doesn't support `... add column if not exists`
	// so we need to check what columns already exist
	existingColumns, err := s.getGolumnNames(table)
	if err != nil {
		return err
	}

	sql := &util.StringBuffer{}
	for column, typ := range jsonTypes {
		if existingColumns[strings.ToLower(column)] {
			continue
		}

		if s, ok := jsonToPgType[typ]; ok {
			typ = s
		}

		// TODO: Should we specify defaults, or just use null as default?
		sql.Reset().
			Printf(`alter table`).
			Space().
			Printf(s.sqlSchemaDatabasePfx).
			SQLIdent(table).
			Space().
			Printf(`add column`).
			Space().
			SQLIdent(column).
			Space().
			SQLIdent(typ)
		_, err := s.db.Exec(sql.String())
		if err != nil {
			return fmt.Errorf("createColumns: %#q: %w", sql.String(), err)
		}
	}
	return nil
}

// decodeBatch decodes up to s.InsertBatchSize messages from stream
// for each messages, each column is extracted and appended to params
// i.e. params is [msg1.a, msg1.b, msg1.c, msg2.a, msg2.b, msg2.c]
//
// paramsBuf is a buffer that can be re-used for the returned params slice
func (s *RedshiftServer) decodeBatch(paramsBuf []any, stream *json.Decoder, columns []string) (params []any, err error) {
	clear(paramsBuf)
	params = paramsBuf[:0]

	size := max(1, s.InsertBatchSize)
	data := map[string]any{}
	for ; size > 0; size-- {
		clear(data)
		if err := stream.Decode(&data); err != nil {
			return params, err
		}
		for _, col := range columns {
			params = append(params, data[col])
		}
	}
	return params, nil
}

// insertBatch inserts a batch of messages
//
// sqlBuf is a buffer used to generate the query
//
// table is the table name
//
// columns is the column names
//
// params is the list of params as returned by decodeBatch
func (s *RedshiftServer) insertBatch(sqlBuf *util.StringBuffer, table string, columns []string, params []any) error {
	if len(params) == 0 {
		return nil
	}

	sqlBuf.Reset().
		Printf(`insert into`).
		Space().
		Printf(s.sqlSchemaDatabasePfx).
		SQLIdent(table).
		Space().
		Printf(`(`)
	for i, column := range columns {
		sqlBuf.PrintfIf(i > 0, ", ")
		sqlBuf.SQLIdent(column)
	}
	sqlBuf.
		Printf(`)`).
		Space().
		Printf(`values`).
		Space()
	for i := 0; i < len(params); i += len(columns) {
		sqlBuf.PrintfIf(i > 0, `, `)
		sqlBuf.Printf(`(`)
		for j := 0; j < len(columns); j++ {
			sqlBuf.PrintfIf(j > 0, `, `)
			sqlBuf.Printf(`$%d`, i+j+1 /*params are 1- not 0- based */)
		}
		sqlBuf.Printf(`)`)
	}
	query := sqlBuf.String()

	_, err := s.db.Exec(sqlBuf.String(), params...)
	log.Debug().
		Err(err).
		Str("query", query).
		Int("rows", len(params)/len(columns)).
		Msg("RedshiftServer.insertBatch")
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

// InsertBatchFromNDJson implements destination.DatabaseServer.InsertBatchFromNDJson
func (s *RedshiftServer) InsertBatchFromNDJson(table string, input io.ReadSeeker) error {
	types, err := util.GetJSONTypes(input)
	if err != nil {
		return err
	}
	// rewind input after it was read by GetJSONTypes
	input.Seek(0, 0)

	if err := s.createTable(table); err != nil {
		return err
	}

	if err := s.createColumns(table, types); err != nil {
		return err
	}

	columns := make([]string, 0, len(types))
	for column := range types {
		columns = append(columns, column)
	}

	sqlBuf := &util.StringBuffer{}
	paramsBuf := []any{}
	stream := json.NewDecoder(input)
	for {
		params, batchErr := s.decodeBatch(paramsBuf, stream, columns)

		// process batch even if there as an error, it's most likely EOF
		insertErr := s.insertBatch(sqlBuf, table, columns, params)

		switch {
		case insertErr != nil:
			return fmt.Errorf("RedshiftServer.InsertBatchFromNDJson: %w", err)
		case errors.Is(batchErr, io.EOF):
			return nil
		case batchErr != nil:
			return fmt.Errorf("RedshiftServer.InsertBatchFromNDJson: %w", err)
		}
	}
}
