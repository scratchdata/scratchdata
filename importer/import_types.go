package importer

import (
	"fmt"
	"scratchdb/apikeys"
	"scratchdb/servers"
	"strings"

	"github.com/rs/zerolog/log"
)

func (im *Importer) getColumnsLocalWithTypes(localPath string) (map[string]string, error) {
	return nil, nil
}

func (im *Importer) createColumnsWithTypes(server servers.ClickhouseServer, user apikeys.APIKeyDetails, table string, columns map[string]string) error {
	sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" `, user.GetDBName(), table)
	columnSql := make([]string, len(columns))
	for colName, colType := range columns {
		log.Print(colName, colType)
		// columnSql[i] = fmt.Sprintf(`ADD COLUMN IF NOT EXISTS "%s" String`, im.renameColumn(column))
	}

	sql += strings.Join(columnSql, ", ")
	return im.executeSQL(server, sql)
}

func (im *Importer) insertDataLocalWithTypes(server servers.ClickhouseServer, user apikeys.APIKeyDetails, localFile, table string, columns map[string]string) error {
	// insertSql := fmt.Sprintf(`INSERT INTO "%s"."%s" (`, user.GetDBName(), table)

	// for colName, colType := range columns {
	// 	insertSql += fmt.Sprintf("`%s`", im.renameColumn(column))
	// 	if i < len(columns)-1 {
	// 		insertSql += ","
	// 	}
	// }
	// insertSql += ")"

	// conn, err := server.Connection()
	// if err != nil {
	// 	return err
	// }

	// batch, err := conn.PrepareBatch(context.Background(), insertSql)
	// if err != nil {
	// 	log.Err(err).Msg("unable to initiate batch query")
	// 	return err
	// }

	// file, err := os.Open(localFile)
	// if err != nil {
	// 	return err
	// }
	// defer file.Close()

	// scanner := bufio.NewScanner(file)
	// maxCapacity := 100_000_000
	// buf := make([]byte, maxCapacity)
	// scanner.Buffer(buf, maxCapacity)

	// for scanner.Scan() {

	// 	data, err := ajson.Unmarshal([]byte(scanner.Text()))
	// 	if err != nil {
	// 		batch.Abort()
	// 		log.Err(err).Msg("error parsing json")
	// 		return err
	// 	}

	// 	nodes, err := data.JSONPath("$")
	// 	for _, node := range nodes {
	// 		vals := make([]interface{}, len(columns))
	// 		for i, c := range columns {
	// 			v, err := node.GetKey(c)
	// 			if err != nil {
	// 				vals[i] = ""
	// 			} else {
	// 				if v.IsString() {
	// 					vals[i], err = strconv.Unquote(v.String())
	// 					if err != nil {
	// 						batch.Abort()
	// 						return err
	// 					}
	// 				} else {
	// 					vals[i] = v.String()
	// 				}
	// 			}
	// 		}
	// 		batch.Append(vals...)
	// 	}
	// }

	// if err := scanner.Err(); err != nil {
	// 	log.Err(err).Msg("scanner error")
	// 	batch.Abort()
	// 	return err
	// }

	// return batch.Send()
	return nil
}
