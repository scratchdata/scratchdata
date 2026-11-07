package bigquery

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/scratchdata/scratchdata/models"
)

func sampleTargetTypes() map[string]bigquery.FieldType {
	return map[string]bigquery.FieldType{
		"x":     bigquery.StringFieldType,
		"count": bigquery.IntegerFieldType,
		"ratio": bigquery.FloatFieldType,
		"flag":  bigquery.BooleanFieldType,
	}
}

func decodeCoercedLine(t *testing.T, input string) map[string]any {
	t.Helper()
	line, err := coerceNDJSONLine([]byte(input), sampleTargetTypes())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(line, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	return parsed
}

func TestResolveColumnTypesPreservesExistingTableSchema(t *testing.T) {
	existing := []models.Column{
		{Name: "__row_id", Type: "INTEGER"},
		{Name: "x", Type: "STRING"},
	}
	inferred := map[string]bigquery.FieldType{
		"x": bigquery.BooleanFieldType,
		"y": bigquery.IntegerFieldType,
	}
	resolved := resolveColumnTypes(existing, inferred)
	if resolved["x"] != bigquery.StringFieldType {
		t.Fatalf("expected existing column x to remain STRING, got %v", resolved["x"])
	}
	if resolved["y"] != bigquery.IntegerFieldType {
		t.Fatalf("expected new column y to be INTEGER, got %v", resolved["y"])
	}
}

func TestCoerceNDJSONLineValidConversions(t *testing.T) {
	parsed := decodeCoercedLine(t, `{"x":true,"count":"42","ratio":"3.14","flag":1}`)
	if parsed["x"] != "true" {
		t.Fatalf("expected x='true', got %v", parsed["x"])
	}
	if parsed["count"] != float64(42) {
		t.Fatalf("expected count=42, got %v", parsed["count"])
	}
	if parsed["ratio"] != 3.14 {
		t.Fatalf("expected ratio=3.14, got %v", parsed["ratio"])
	}
	if parsed["flag"] != true {
		t.Fatalf("expected flag=true, got %v", parsed["flag"])
	}
}

func TestCoerceNDJSONLineInvalidFallbackToNull(t *testing.T) {
	parsed := decodeCoercedLine(t, `{"x":123,"count":"not_an_int","ratio":true,"flag":"invalid"}`)
	if parsed["x"] != "123" {
		t.Fatalf("expected x='123', got %v", parsed["x"])
	}
	for _, field := range []string{"count", "ratio", "flag"} {
		if parsed[field] != nil {
			t.Fatalf("expected %s to become nil, got %v", field, parsed[field])
		}
	}
}

func TestCoerceNDJSONFileEndToEnd(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "input-*.ndjson")
	if err != nil {
		t.Fatalf("temp file error: %v", err)
	}
	_, _ = tmp.WriteString("{\"x\": \"y\"}\n{\"x\": true}\n")
	_ = tmp.Close()

	outPath, err := coerceNDJSONFile(tmp.Name(), map[string]bigquery.FieldType{
		"x": bigquery.StringFieldType,
	})
	if err != nil {
		t.Fatalf("coerceNDJSONFile failed: %v", err)
	}
	defer os.Remove(outPath)

	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if !strings.Contains(lines[1], `"x":"true"`) {
		t.Fatalf("unexpected coerced line: %s", lines[1])
	}
}
