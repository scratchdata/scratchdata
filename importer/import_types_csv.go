package importer

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"scratchdb/apikeys"
	"scratchdb/servers"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
)

// Currently unused
func (im *Importer) insertDataCSV(
	conn driver.Conn,
	insertSql string,
	file *os.File,
	colNames []string,
	clickhouseColumnTypes map[string]string,
	localFile string,
	server servers.ClickhouseServer,
	table string,
	user apikeys.APIKeyDetails,
) error {
	scanner := bufio.NewScanner(file)
	maxCapacity := 100_000_000
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	csv, err := os.CreateTemp("", "")
	if err != nil {
		return err
	}
	// defer os.Remove(csv.Name())
	log.Print(csv.Name())

	for _, colName := range colNames {
		_, err = csv.WriteString(im.renameColumn(colName) + "\t")
		if err != nil {
			return err
		}
	}

	_, err = csv.WriteString("\n")
	if err != nil {
		return err
	}

	// Iterate over each JSON object
	for scanner.Scan() {
		data := scanner.Bytes()

		for _, colName := range colNames {
			_, err = csv.Write([]byte(gjson.GetBytes(data, colName).String()))
			if err != nil {
				return err
			}
			_, err = csv.WriteString("\t")
			if err != nil {
				return err
			}

			// colType := clickhouseColumnTypes[im.renameColumn(colName)]
			// vals[i] = im.jsonToGoType(colType, gjson.GetBytes(data, colName))
		}

		_, err = csv.WriteString("\n")
		if err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		log.Err(err).Msg("scanner error")
		return err
	}

	_, err = csv.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	baseURL := fmt.Sprintf("%s://%s:%d", server.GetHttpProtocol(), server.GetHost(), server.GetHttpPort())
	sql := fmt.Sprintf("INSERT INTO \"%s\" FORMAT TSVWithNames", table)

	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return err
	}

	query := parsedURL.Query()
	query.Set("query", sql)

	parsedURL.RawQuery = query.Encode()
	req, err := http.NewRequest("POST", parsedURL.String(), csv)
	if err != nil {
		return err
	}

	// 	// Set the content type as application/json
	// 	// req.Header.Set("Content-Type", "application/json")

	req.Header.Set("X-Clickhouse-User", user.GetDBUser())
	req.Header.Set("X-Clickhouse-Key", user.GetDBPassword())
	req.Header.Set("X-Clickhouse-Database", user.GetDBName())

	// Create a new HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		msg, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(msg))
	}

	return nil
}
