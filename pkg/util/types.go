package util

import (
	"bufio"
	"io"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
)

func GetJSONTypes(file io.ReadSeeker) (map[string]string, error) {
	rc := map[string]string{}
	typeCounts := map[string]map[string]int{}

	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return rc, err
	}

	scanner := bufio.NewScanner(file)
	maxCapacity := 100_000_000
	buf := make([]byte, 2_000)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		parsed := gjson.ParseBytes(scanner.Bytes())

		parsed.ForEach(func(key, value gjson.Result) bool {
			k := key.String()
			_, ok := typeCounts[k]
			if !ok {
				typeCounts[k] = map[string]int{}
			}
			switch value.Type {
			case gjson.String:
				typeCounts[k]["string"] += 1
			case gjson.Null:
				typeCounts[k]["null"] += 1
			case gjson.False:
				typeCounts[k]["bool"] += 1
			case gjson.True:
				typeCounts[k]["bool"] += 1
			case gjson.Number:
				_, intErr := strconv.Atoi(value.Raw)
				if intErr != nil {
					typeCounts[k]["float"] += 1
				} else {
					typeCounts[k]["int"] += 1
				}
			default:
				typeCounts[k]["undefined"] += 1
			}
			return true
		})

	}

	log.Trace().Interface("column_type_counts", typeCounts).Send()

	if err := scanner.Err(); err != nil {
		return rc, err
	}

	for colName, colTypeCounts := range typeCounts {
		if colTypeCounts["string"] > 0 {
			rc[colName] = "string"
			continue
		} else if colTypeCounts["undefined"] > 0 {
			rc[colName] = "string"
			continue
		} else if colTypeCounts["float"] > 0 {
			rc[colName] = "float"
			continue
		} else if colTypeCounts["int"] > 0 {
			rc[colName] = "int"
			continue
		} else if colTypeCounts["bool"] > 0 {
			rc[colName] = "bool"
		} else {
			rc[colName] = "string"
		}

	}

	log.Trace().Interface("column_types", rc).Send()

	return rc, nil
}
