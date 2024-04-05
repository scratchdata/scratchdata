package api

import (
	"log"
	"testing"
)

func TestMultiTableFlattener(t *testing.T) {
	json_str := `{
		"name": "John Doe",
		"age": 30,
		"address": {
			"street": "123 Main St",
			"city": "Anytown"
		},
		"hobbies": [
			{"name": "Reading", "type": "Indoor"},
			{"name": "Cycling", "type": "Outdoor", "nested": {"scalar": "bar", "list": [1,2], "obj": {"hello":"world"}}}
		],
		"numbers": [11, 22, 33]
	}`

	f := NewMultiTableFlattener()
	rc, _ := f.Flatten("t", json_str)
	for _, v := range rc {
		log.Println(v.Table, v.JSON)
	}
}
