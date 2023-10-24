package ingest

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremywohl/flatten"
	"github.com/spyzhov/ajson"
)

func parseMap(obj map[string]interface{}, path []string, useIndices bool) [][]map[string]interface{} {
	var result [][]map[string]interface{}
	for k, v := range obj {
		result = append(result, Flatten(v, append(path, k), useIndices))
	}
	return result
}

func crossProduct(dicts [][]map[string]interface{}) []map[string]interface{} {
	if len(dicts) == 0 {
		return []map[string]interface{}{{}}
	}
	var result []map[string]interface{}
	for _, lhs := range dicts[0] {
		for _, rhs := range crossProduct(dicts[1:]) {
			result = append(result, merge(lhs, rhs))
		}
	}
	return result
}

func merge(lhs, rhs map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range lhs {
		result[k] = v
	}
	for k, v := range rhs {
		result[k] = v
	}
	return result
}

func pathToString(path []string) string {
	var result string
	for i, p := range path {
		if i > 0 {
			result += "_"
		}
		result += p
	}
	return result
}

func Flatten(obj interface{}, path []string, useIndices bool) []map[string]interface{} {
	switch obj := obj.(type) {
	case []interface{}:
		if len(obj) > 0 {
			if useIndices {
				var result []map[string]interface{}
				for i, item := range obj {
					result = append(result, Flatten(item, append(path, strconv.Itoa(i)), useIndices)...)
				}
				return result
			}
			var result []map[string]interface{}
			for _, item := range obj {
				result = append(result, Flatten(item, path, useIndices)...)
			}
			return result
		} else {
			return []map[string]interface{}{{
				pathToString(path): nil,
			}}
		}
	case map[string]interface{}:
		return crossProduct(parseMap(obj, path, useIndices))
	default:
		return []map[string]interface{}{{
			pathToString(path): obj,
		}}
	}
}

func FlattenJSON(obj string, path []string, useIndices bool) ([]string, error) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(obj), &data)
	if err != nil {
		return nil, err
	}

	flattened := Flatten(data, path, useIndices)

	rc := make([]string, 0)
	for _, f := range flattened {
		b, err := json.Marshal(f)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		rc = append(rc, string(b))
	}

	return rc, nil
}

func flattenArray(algo string, node *ajson.Node, writer *FileWriter) error {
	objects, err := node.GetArray()
	if err != nil {
		return err
	}
	for _, o := range objects {

		if algo == "explode" {
			flats, err := FlattenJSON(o.String(), nil, false)
			if err != nil {
				return err
			}

			for _, flat := range flats {
				err = writer.Write(flat)
				if err != nil {
					log.Println("Unable to write object", flat, err)
				}

			}

		} else {
			flat, err := flatten.FlattenString(o.String(), "", flatten.UnderscoreStyle)
			if err != nil {
				return err
			}
			err = writer.Write(flat)
			if err != nil {
				log.Println("Unable to write object", flat, err)
			}
		}
	}
	return nil
}

func flattenObject(algo string, node *ajson.Node, writer *FileWriter) error {
	if algo == "explode" {
		flats, err := FlattenJSON(node.String(), nil, false)
		if err != nil {
			return err
		}

		for _, flat := range flats {
			err = writer.Write(flat)
			if err != nil {
				log.Println("Unable to write object", flat, err)
			}

		}

	} else {
		flat, err := flatten.FlattenString(node.String(), "", flatten.UnderscoreStyle)
		if err != nil {
			return err
		}

		err = writer.Write(flat)
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}
	}
	return nil
}
