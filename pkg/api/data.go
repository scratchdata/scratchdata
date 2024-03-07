package api

import (
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	oldapi "github.com/scratchdata/scratchdata/cmd/api"
)

func (a *ScratchDataAPIStruct) Select(w http.ResponseWriter, r *http.Request) {
	databaseID := a.AuthGetDatabaseID(r.Context())
	var query string

	query = r.URL.Query().Get("query")
	if r.Method == "POST" {
		queryBytes, err := io.ReadAll(r.Body)
		if err != nil && len(queryBytes) > 0 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Unable to read query"))
			return
		}
		query = string(queryBytes)
	}

	if strings.TrimSpace(query) == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Query cannot be blank"))
		return
	}

	dest, err := a.storageServices.Destination(databaseID)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Unable to connect to database"))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	dest.QueryJSON(query, w)
}

func (a *ScratchDataAPIStruct) Insert(w http.ResponseWriter, r *http.Request) {
	databaseID := a.AuthGetDatabaseID(r.Context())
	table := chi.URLParam(r, "table")
	flatten := r.URL.Query().Get("flatten")

	var flattener oldapi.Flattener
	if flatten == "vertical" {
		flattener = oldapi.ExplodeFlattener{}
	} else {
		flattener = oldapi.HorizontalFlattener{}
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Unable to read data"))
		return
	}

	if !gjson.ValidBytes(body) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid JSON"))
		return
	}

	parsed := gjson.ParseBytes(body)

	parsed.IsArray()
	lines := parsed.Array()

	errorItems := map[int]bool{}
	for i, line := range lines {
		flatItems, err := flattener.Flatten(table, line.Raw)
		if err != nil {
			errorItems[i] = true
			log.Trace().Err(err).Str("json", line.Raw).Msg("Unable to flatten JSON")
			continue
		}

		for _, flatItem := range flatItems {
			var writeErr error
			var toWrite string

			toWrite = flatItem.JSON

			if !gjson.Get(flatItem.JSON, "__row_id").Exists() {
				snowID := a.snow.Generate()
				rowID := snowID.Int64()
				if toWrite, err = sjson.Set(flatItem.JSON, "__row_id", rowID); err != nil {
					log.Trace().Err(err).Str("json", flatItem.JSON).Msg("Unable to add __row_id")
				}
			}

			writeErr = a.storageServices.DataSink().WriteData(databaseID, flatItem.Table, []byte(toWrite))

			if writeErr != nil {
				errorItems[i] = true
				log.Trace().Err(err).Str("json", flatItem.JSON).Msg("Unable to write JSON")
			}
		}
	}

	if len(errorItems) > 0 {
		if len(errorItems) == len(lines) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Unable to insert data"))
			return
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Partially inserted data"))
			return
		}
	}

	w.Write([]byte("ok"))
}
