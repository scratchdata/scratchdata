package memory

import (
	"io"
	"scratchdata/pkg/destinations/duckdb"
)

// Wrapper around DuckDB's in-memory server
type MemoryDBServer struct {
	s *duckdb.DuckDBServer
}

func (m *MemoryDBServer) InsertBatchFromNDJson(table string, r io.ReadSeeker) error {
	return m.s.InsertBatchFromNDJson(table, r)
}

func (m *MemoryDBServer) QueryJSON(query string, w io.Writer) error {
	return m.s.QueryJSON(query, w)
}

func (m *MemoryDBServer) Close() error {
	return m.s.Close()
}

func OpenServer() (*MemoryDBServer, error) {
	duckdbConfig := map[string]any{"memory": true}

	duckdbServer, err := duckdb.OpenServer(duckdbConfig)
	if err != nil {
		return nil, err
	}

	server := &MemoryDBServer{
		s: duckdbServer,
	}

	return server, nil
}
