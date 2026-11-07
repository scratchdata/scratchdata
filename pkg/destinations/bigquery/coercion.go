package bigquery

import (
	"bufio"
	"encoding/json"
	"io"
	"math"
	"os"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/scratchdata/scratchdata/models"
)

// ndjsonTranscodeStreams groups the input reader and output writer for NDJSON schema coercion.
type ndjsonTranscodeStreams struct {
	reader io.Reader
	writer io.Writer
}

// resolveColumnTypes merges existing BigQuery table column types with newly inferred JSON types.
func resolveColumnTypes(existing []models.Column, inferred map[string]bigquery.FieldType) map[string]bigquery.FieldType {
	resolved := make(map[string]bigquery.FieldType, len(inferred))
	for col, fType := range inferred {
		resolved[col] = fType
	}
	for _, col := range existing {
		if col.Name == "__row_id" {
			continue
		}
		if _, present := resolved[col.Name]; present {
			resolved[col.Name] = normalizeBQFieldType(col.Type)
		}
	}
	return resolved
}

func normalizeBQFieldType(rawType string) bigquery.FieldType {
	switch strings.ToUpper(strings.TrimSpace(rawType)) {
	case "INTEGER", "INT64", "INT":
		return bigquery.IntegerFieldType
	case "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC":
		return bigquery.FloatFieldType
	case "BOOLEAN", "BOOL":
		return bigquery.BooleanFieldType
	default:
		return bigquery.StringFieldType
	}
}

// coerceNDJSONFile rewrites an NDJSON file so every column value matches its target BigQuery FieldType or becomes null.
func coerceNDJSONFile(srcPath string, targetTypes map[string]bigquery.FieldType) (string, error) {
	inFile, err := os.Open(srcPath)
	if err != nil {
		return "", err
	}
	defer inFile.Close()

	outFile, err := os.CreateTemp(os.TempDir(), "scratchdata-bq-coerced-*.ndjson")
	if err != nil {
		return "", err
	}
	outPath := outFile.Name()
	streams := ndjsonTranscodeStreams{reader: inFile, writer: outFile}
	if err := transcodeNDJSONStream(streams, targetTypes); err != nil {
		_ = outFile.Close()
		_ = os.Remove(outPath)
		return "", err
	}
	if err := outFile.Close(); err != nil {
		_ = os.Remove(outPath)
		return "", err
	}
	return outPath, nil
}

func transcodeNDJSONStream(streams ndjsonTranscodeStreams, targetTypes map[string]bigquery.FieldType) error {
	scanner := bufio.NewScanner(streams.reader)
	buf := make([]byte, 2000)
	scanner.Buffer(buf, 100_000_000)
	writer := bufio.NewWriter(streams.writer)
	for scanner.Scan() {
		coercedLine, err := coerceNDJSONLine(scanner.Bytes(), targetTypes)
		if err != nil {
			return err
		}
		if len(coercedLine) == 0 {
			continue
		}
		if _, err := writer.Write(append(coercedLine, '\n')); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return writer.Flush()
}

// coerceNDJSONLine converts a single JSON object line to match the target BigQuery column schema.
func coerceNDJSONLine(line []byte, targetTypes map[string]bigquery.FieldType) ([]byte, error) {
	trimmed := strings.TrimSpace(string(line))
	if trimmed == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	var record map[string]any
	if err := decoder.Decode(&record); err != nil {
		return nil, err
	}
	for colName, val := range record {
		if targetType, ok := targetTypes[colName]; ok {
			record[colName] = coerceValueForBQType(val, targetType)
		}
	}
	return json.Marshal(record)
}

// coerceValueForBQType applies BigQuery Standard SQL conversion rules or returns nil when incompatible.
func coerceValueForBQType(val any, targetType bigquery.FieldType) any {
	if val == nil {
		return nil
	}
	switch targetType {
	case bigquery.StringFieldType:
		return coerceToString(val)
	case bigquery.IntegerFieldType:
		return coerceToInt64(val)
	case bigquery.FloatFieldType:
		return coerceToFloat64(val)
	case bigquery.BooleanFieldType:
		return coerceToBool(val)
	default:
		return coerceToString(val)
	}
}

func coerceToString(val any) any {
	switch v := val.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return nil
	}
}

func coerceToInt64(val any) any {
	switch v := val.(type) {
	case json.Number:
		return parseJSONNumberAsInt64(v)
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return roundFloatToInt64(v)
	case bool:
		return boolToInt64(v)
	case string:
		return parseStringAsInt64(v)
	default:
		return nil
	}
}

func parseJSONNumberAsInt64(num json.Number) any {
	if i, err := num.Int64(); err == nil {
		return i
	}
	if f, err := num.Float64(); err == nil {
		return roundFloatToInt64(f)
	}
	return nil
}

func roundFloatToInt64(f float64) any {
	if !isFiniteFloat(f) {
		return nil
	}
	return int64(math.Round(f))
}

func isFiniteFloat(f float64) bool {
	if math.IsNaN(f) {
		return false
	}
	return !math.IsInf(f, 0)
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func parseStringAsInt64(raw string) any {
	parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return nil
	}
	return parsed
}

func coerceToFloat64(val any) any {
	switch v := val.(type) {
	case json.Number:
		return parseStringAsFloat64(v.String())
	case float64:
		if !isFiniteFloat(v) {
			return nil
		}
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		return parseStringAsFloat64(v)
	default:
		return nil
	}
}

func parseStringAsFloat64(raw string) any {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return nil
	}
	if !isFiniteFloat(parsed) {
		return nil
	}
	return parsed
}

func coerceToBool(val any) any {
	switch v := val.(type) {
	case bool:
		return v
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i != 0
		}
		return nil
	case int:
		return v != 0
	case int64:
		return v != 0
	case string:
		return parseStringAsBool(v)
	default:
		return nil
	}
}

func parseStringAsBool(raw string) any {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "true":
		return true
	case "false":
		return false
	default:
		return nil
	}
}
