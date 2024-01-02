package util

import (
	"testing"
)

func TestConfigToStruct(t *testing.T) {
	type T struct {
		X string `mapstructure:"x"`
	}

	// make sure a valid value is returned when no config is provided
	v := ConfigToStruct[T](nil)
	if v == nil {
		t.Fatal("ConfigToStruct returned nil")
	}

	// make sure a valid value is returned when invalid config is provided
	v = ConfigToStruct[T](map[string]any{"x": 123})
	if v == nil {
		t.Fatal("ConfigToStruct returned nil")
	}

	v = ConfigToStruct[T](map[string]any{"x": "y"})
	if v.X != "y" {
		t.Fatalf("Expected y`; Got %#q", v.X)
	}
}
