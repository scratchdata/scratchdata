package api

import (
	"encoding/json"

	"github.com/bwmarrin/snowflake"
	"github.com/jeremywohl/flatten"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type JSONData struct {
	Table string
	JSON  string
}

type Flattener interface {
	Flatten(table string, json string) ([]JSONData, error)
}

type VerticalFlattener struct{}

func (e VerticalFlattener) parseMap(obj map[string]interface{}, path []string, useIndices bool) [][]map[string]interface{} {
	var result [][]map[string]interface{}
	for k, v := range obj {
		result = append(result, e.flattenObject(v, append(path, k), useIndices))
	}
	return result
}

func (e VerticalFlattener) crossProduct(dicts [][]map[string]interface{}) []map[string]interface{} {
	if len(dicts) == 0 {
		return []map[string]interface{}{{}}
	}
	var result []map[string]interface{}
	for _, lhs := range dicts[0] {
		for _, rhs := range e.crossProduct(dicts[1:]) {
			result = append(result, e.merge(lhs, rhs))
		}
	}
	return result
}

func (e VerticalFlattener) merge(lhs, rhs map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range lhs {
		result[k] = v
	}
	for k, v := range rhs {
		result[k] = v
	}
	return result
}

func (e VerticalFlattener) pathToString(path []string) string {
	var result string
	for i, p := range path {
		if i > 0 {
			result += "_"
		}
		result += p
	}
	return result
}

func (e VerticalFlattener) flattenObject(obj interface{}, path []string, useIndices bool) []map[string]interface{} {
	switch obj := obj.(type) {
	case []interface{}:
		if len(obj) > 0 {
			var result []map[string]interface{}
			for i, item := range obj {
				newItems := e.flattenObject(item, path, useIndices)

				if useIndices {
					for _, newItem := range newItems {
						newItem["__order_"+e.pathToString(path)] = i
					}
				}

				result = append(result, newItems...)
			}
			return result
		} else {
			return []map[string]interface{}{{
				e.pathToString(path): nil,
			}}
		}
	case map[string]interface{}:
		return e.crossProduct(e.parseMap(obj, path, useIndices))
	default:
		return []map[string]interface{}{{
			e.pathToString(path): obj,
		}}
	}
}

func (e VerticalFlattener) flattenJSON(obj string, path []string, useIndices bool) ([]string, error) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(obj), &data)
	if err != nil {
		return nil, err
	}

	flattened := e.flattenObject(data, path, useIndices)

	rc := make([]string, 0)
	for _, f := range flattened {
		b, err := json.Marshal(f)
		if err != nil {
			log.Err(err).Msg("failed to marshal json")
			return nil, err
		}
		rc = append(rc, string(b))
	}

	return rc, nil
}

func (e VerticalFlattener) Flatten(table string, json string) ([]JSONData, error) {
	documentId := ulid.Make().String()
	dataWithDocumentId, err := sjson.Set(json, "___document_id", documentId)

	var flattened []string
	if err == nil {
		flattened, err = e.flattenJSON(dataWithDocumentId, nil, true)
	} else {
		flattened, err = e.flattenJSON(json, nil, true)
	}

	if err != nil {
		return nil, err
	}

	rc := make([]JSONData, len(flattened))
	for i, data := range flattened {
		rc[i].Table = table
		rc[i].JSON = data
	}
	return rc, nil
}

type HorizontalFlattener struct{}

func (h HorizontalFlattener) Flatten(table string, json string) ([]JSONData, error) {
	flat, err := flatten.FlattenString(json, "", flatten.UnderscoreStyle)
	if err != nil {
		return nil, err
	}

	rc := []JSONData{
		{Table: table, JSON: flat},
	}

	return rc, nil
}

type MultiTableFlattener struct {
	snowflake *snowflake.Node
}

func NewMultiTableFlattener() MultiTableFlattener {
	snowflake, _ := util.NewSnowflakeGenerator()
	return MultiTableFlattener{snowflake: snowflake}
}

func (f MultiTableFlattener) FlattenJSON(table string, data gjson.Result, parent_table string, parent_id int64, output []JSONData) {
	if data.IsObject() {
		oid := f.snowflake.Generate().Int64()
		rc := map[string]any{
			"id":  oid,
			table: data.Value(),
		}
		if parent_table != "" {
			rc[parent_table+"_id"] = parent_id
		}
		j, _ := json.Marshal(rc)

		data.ForEach(func(key, value gjson.Result) bool {
			if value.IsArray() || value.IsObject() {
				f.FlattenJSON(key.String(), value, table, oid, output)

			} else {
				rc[key.String()] = value.Value()
			}

			return true // keep iterating
		})

		output = append(output, JSONData{Table: table, JSON: string(j)})

	} else if data.IsArray() {
		for _, v := range data.Array() {
			f.FlattenJSON(table, v, parent_table, parent_id, output)
		}
	} else {
		rc := map[string]any{
			"id":  f.snowflake.Generate().Int64(),
			table: data.Value(),
		}
		if parent_table != "" {
			rc[parent_table+"_id"] = parent_id
		}
		j, _ := json.Marshal(rc)
		output = append(output, JSONData{Table: table, JSON: string(j)})
	}

}

func (f MultiTableFlattener) Flatten(table string, json string) ([]JSONData, error) {
	rc := []JSONData{}

	parsed := gjson.Parse(json)
	f.FlattenJSON(table, parsed, "", 0, rc)

	log.Print("READY")
	log.Print(rc)
	log.Print("DONE")

	return rc, nil
	// flat, err := flatten.FlattenString(json, "", flatten.UnderscoreStyle)
	// if err != nil {
	// 	return nil, err
	// }

	// rc := []JSONData{
	// 	{Table: table, JSON: flat},
	// }

	// return rc, nil
}
