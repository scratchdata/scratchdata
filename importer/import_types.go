package importer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"

	"os"
	"scratchdb/apikeys"
	"scratchdb/servers"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/tidwall/gjson"
)

func (im *Importer) jsonToGoType(clickhouseType string, data gjson.Result) any {
	switch clickhouseType {
	case "String", "FixedString":
		return data.String()
	case "Decimal":
		return decimal.NewFromFloat(data.Float())
	case "Bool":
		return data.Bool()
	case "UInt8":
		return uint8(data.Uint())
	case "UInt16":
		return uint16(data.Uint())
	case "UInt32":
		return uint32(data.Uint())
	case "UInt64":
		return data.Uint()
	case "UInt128", "UInt256":
		n := new(big.Int)
		n.SetString(data.String(), 10)
		return n
	case "Int8":
		return int8(data.Int())
	case "Int16":
		return int16(data.Int())
	case "Int32":
		return int32(data.Int())
	case "Int64":
		return data.Int()
	case "Int128", "Int256":
		n := new(big.Int)
		n.SetString(data.String(), 10)
		return n
	case "Float32":
		return float32(data.Float())
	case "Float64":
		return data.Float()
	case "UUID":
		return data.String()
	case "Date", "Date32":
		return data.String()
	case "DateTime", "DateTime64":
		if data.Type == gjson.Number {
			return data.Int()
		} else {
			return data.String()
		}
	case "Enum8":
		return int8(data.Int())
	case "Enum16":
		return int16(data.Int())
	}

	return data.String()
}

func (im *Importer) getClickhouseTypes(server servers.ClickhouseServer, user apikeys.APIKeyDetails, table string) (map[string]string, error) {
	rc := map[string]string{}

	baseURL := fmt.Sprintf("%s://%s:%d", server.GetHttpProtocol(), server.GetHost(), server.GetHttpPort())
	sql := fmt.Sprintf("DESCRIBE TABLE \"%s\" FORMAT JSON", table)

	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return rc, err
	}

	query := parsedURL.Query()
	query.Set("query", sql)

	parsedURL.RawQuery = query.Encode()
	req, err := http.NewRequest("GET", parsedURL.String(), nil)
	if err != nil {
		return rc, err
	}

	// Set the content type as application/json
	// req.Header.Set("Content-Type", "application/json")

	req.Header.Set("X-Clickhouse-User", user.GetDBUser())
	req.Header.Set("X-Clickhouse-Key", user.GetDBPassword())
	req.Header.Set("X-Clickhouse-Database", user.GetDBName())

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return rc, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return rc, err
	}

	parsed := gjson.ParseBytes(data)
	for _, field := range parsed.Get("data").Array() {
		rc[field.Get("name").String()] = field.Get("type").String()
	}

	log.Trace().Interface("clickhouse_column_types", rc).Str("table", table).Send()
	return rc, nil
}

func (im *Importer) getColumnsLocalWithTypes(fileName string) (map[string]string, error) {
	rc := map[string]string{}

	typeCounts := map[string]map[string]int{}

	file, err := os.Open(fileName)
	if err != nil {
		return rc, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	maxCapacity := 100_000_000
	buf := make([]byte, maxCapacity)
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

	log.Trace().Interface("column_type_counts", typeCounts).Str("key", fileName).Send()

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

	log.Trace().Interface("column_types", rc).Str("key", fileName).Send()

	return rc, nil
}

func (im *Importer) createColumnsWithTypes(server servers.ClickhouseServer, user apikeys.APIKeyDetails, table string, columns map[string]string) error {
	sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" `, user.GetDBName(), table)
	columnSql := []string{}
	for colName, jsonType := range columns {
		var colType string
		switch jsonType {
		case "int":
			colType = "Int64"
		case "bool":
			colType = "Boolean"
		case "float":
			colType = "Float64"
		case "string":
			colType = "String"
		default:
			colType = "String"
		}
		columnSql = append(columnSql, fmt.Sprintf(`ADD COLUMN IF NOT EXISTS "%s" %s`, im.renameColumn(colName), colType))
	}

	sql += strings.Join(columnSql, ", ")

	log.Trace().Msg(sql)

	return im.executeSQL(server, sql)
}

func (im *Importer) insertDataLocalWithTypes(server servers.ClickhouseServer, user apikeys.APIKeyDetails, localFile, table string, columns map[string]string) error {
	// Get list of columns so we use the same order
	colNames := make([]string, len(columns))
	i := 0
	for k := range columns {
		colNames[i] = k
		i++
	}

	// Get types for clickhouse columns
	clickhouseColumnTypes, err := im.getClickhouseTypes(server, user, table)
	if err != nil {
		return err
	}

	// Create INSERT statement
	insertSql := fmt.Sprintf(`INSERT INTO "%s"."%s" (`, user.GetDBName(), table)
	for i, colName := range colNames {
		insertSql += fmt.Sprintf("`%s`", im.renameColumn(colName))
		if i < len(columns)-1 {
			insertSql += ","
		}
	}
	insertSql += ")"

	log.Trace().Str("key", localFile).Msg(insertSql)

	// Open .ndjson file for reading
	file, err := os.Open(localFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get clickhouse server conn
	conn, err := server.Connection()
	if err != nil {
		return err
	}

	// Begin batch
	batch, err := conn.PrepareBatch(context.Background(), insertSql)
	if err != nil {
		log.Err(err).Msg("unable to initiate batch query")
		return err
	}

	scanner := bufio.NewScanner(file)
	maxCapacity := 100_000_000
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	// Iterate over each JSON object
	row := 0
	for scanner.Scan() {
		data := scanner.Bytes()
		vals := make([]any, len(colNames))

		for i, colName := range colNames {
			colType := clickhouseColumnTypes[im.renameColumn(colName)]
			vals[i] = im.jsonToGoType(colType, gjson.GetBytes(data, colName))
		}

		log.Trace().Interface("vals", vals).Str("key", localFile).Int("row", row).Send()
		err = batch.Append(vals...)
		if err != nil {
			log.Error().Err(err).Str("key", localFile).Int("row", row).Msg("Unable to add item to batch")
		}
		row++
	}

	if err := scanner.Err(); err != nil {
		log.Err(err).Msg("scanner error")
		batch.Abort()
		return err
	}

	return batch.Send()
}
