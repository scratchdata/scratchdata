package redshift

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"scratchdata/util"

	"github.com/rs/zerolog/log"
)

// QueryJSON implements destination.DatabaseServer.QueryJSON
func (s *RedshiftServer) QueryJSON(query string, output io.Writer) (err error) {
	out := bufio.NewWriter(output)
	defer func() {
		if e := out.Flush(); e != nil && err == nil {
			err = e
		}
	}()

	query = (&util.StringBuffer{}).
		Printf(`select * from (%s) as results`, util.TrimQuery(query)).
		String()

	rows, err := s.db.Query(query)
	log.Debug().
		Err(err).
		Msg("RedshiftServer: QueryJSON")
	if err != nil {
		return fmt.Errorf("RedshiftServer.QueryJSON: %w", err)
	}
	defer rows.Close()

	writeIf := func(ok bool, p ...byte) error {
		if !ok {
			return nil
		}
		if _, err := out.Write(p); err != nil {
			return fmt.Errorf("RedshiftServer.QueryJSON: write failed: %w", err)
		}
		return nil
	}

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("RedshiftServer.QueryJSON: Cannot get column: %w", err)
	}
	values := make([]any, len(columns))
	for i := range columns {
		var v any
		values[i] = &v
	}
	msgBuf := bytes.NewBuffer(nil)
	msgEnc := json.NewEncoder(msgBuf)
	msg := make(map[string]any, len(columns))
	for _, col := range columns {
		msg[col] = nil
	}

	firstRow := true
	if err := writeIf(true, '['); err != nil {
		return err
	}
	for rows.Next() {
		if err := writeIf(!firstRow, ','); err != nil {
			return err
		}
		firstRow = false

		if err := rows.Scan(values...); err != nil {
			return fmt.Errorf("RedshiftServer.QueryJSON: Cannot scan row: %w", err)
		}
		for i, v := range values {
			msg[columns[i]] = v
		}

		msgBuf.Reset()
		if err := msgEnc.Encode(msg); err != nil {
			return fmt.Errorf("RedshiftServer.QueryJSON: Cannot encode row: %w", err)
		}

		if _, err := out.Write(bytes.TrimSpace(msgBuf.Bytes())); err != nil {
			return fmt.Errorf("RedshiftServer.QueryJSON: Cannot write row: %w", err)
		}
	}
	if err := writeIf(true, ']'); err != nil {
		return err
	}
	return nil
}
