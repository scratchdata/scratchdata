package api

import (
	"fmt"
	"testing"
)

func TestExplode(t *testing.T) {
	flattener := ExplodeFlattener{}
	data, err := flattener.Flatten("t", `{"hello":["a","b",{"c":1,"d":2}, {"e":["x","y","z"]}], "world": "x", "j": [10,20,30]}`)
	if err != nil {
		t.Error(err)
	}

	for _, d := range data {
		fmt.Println(d.JSON)
	}
}

func TestHorizontal(t *testing.T) {
	flattener := HorizontalFlattener{}
	data, err := flattener.Flatten("t", `{"hello":["a","b",{"c":1,"d":2}, {"e":["x","y","z"]}], "world": "x", "j": [10,20,30]}`)
	if err != nil {
		t.Error(err)
	}

	for _, d := range data {
		fmt.Println(d.JSON)
	}
}
