package duckdb

import (
	"database/sql"
	"fmt"
	"io"
	"scratchdata/util"

	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
)

func (s *DuckDBServer) createTable(table string, db *sql.DB) error {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (__row_id STRING)", table)
	_, err := db.Exec(sql)
	return err
}

func (s *DuckDBServer) createColumns(table string, jsonTypes map[string]string, db *sql.DB) error {
	for colName, jsonType := range jsonTypes {

		// TODO: Should we specify defaults, or just use null as default?
		sql := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table, colName, jsonToDuck[jsonType])
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}

		// sql = fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL", table, colName)
	}

	return nil
}

func (s *DuckDBServer) describeTable(table string, db *sql.DB) ([]string, map[string]string, error) {
	duckColumns := []string{}
	duckdbColTypes := make(map[string]string)

	sql := fmt.Sprintf("DESCRIBE \"%s\"", table)
	rows, err := db.Query(sql)
	if err != nil {
		return duckColumns, duckdbColTypes, err
	}

	for rows.Next() {
		var colName, colType string
		var isNull, key, defaultValue, extra *string
		if err := rows.Scan(&colName, &colType, &isNull, &key, &defaultValue, &extra); err != nil {
			return duckColumns, duckdbColTypes, err
		}

		duckdbColTypes[colName] = colType
		duckColumns = append(duckColumns, colName)
	}

	if err := rows.Err(); err != nil {
		return duckColumns, duckdbColTypes, err
	}

	return duckColumns, duckdbColTypes, err
}

func (s *DuckDBServer) insertFromS3(table string, tempFile string, db *sql.DB) error {
	sql := fmt.Sprintf(`
		INSERT INTO "%s" 
		BY NAME
		SELECT * FROM
		read_ndjson_auto(
			's3://%s/%s?s3_region=%s&s3_access_key_id=%s&s3_secret_access_key=%s&s3_endpoint=%s&s3_use_ssl=true'
		 )
		`,
		table, s.Bucket, tempFile, s.Region, s.AccessKeyId, s.SecretAccessKey, s.Endpoint,
	)

	log.Print(sql)

	return nil
}

func (s *DuckDBServer) InsertBatchFromNDJson(table string, input io.ReadSeeker) error {

	// Infer JSON types for the input
	jsonTypes, err := util.GetJSONTypes(input)
	if err != nil {
		return err
	}

	connector, err := s.getConnector()
	// connector, err := duckdb.NewConnector("md:"+s.Database+"?motherduck_token="+s.Token, func(execer driver.ExecerContext) error {
	// 	bootQueries := []string{
	// 		"INSTALL 'json'",
	// 		"LOAD 'json'",
	// 	}

	// 	for _, qry := range bootQueries {
	// 		_, err = execer.ExecContext(context.TODO(), qry, nil)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// })
	if err != nil {
		return err
	}

	// duckdb.NewConnector()
	// conn, err := connector.Connect(context.TODO())
	// db, err := sql.Open("duckdb", "md:"+s.Database+"?motherduck_token="+s.Token)
	db := sql.OpenDB(connector)
	if err != nil {
		return err
	}
	defer db.Close()

	err = s.createTable(table, db)
	if err != nil {
		return err
	}

	err = s.createColumns(table, jsonTypes, db)
	// for colName, jsonType := range jsonTypes {
	// 	sql = fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table, colName, jsonToDuck[jsonType])
	// 	_, err = db.Exec(sql)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	sql = fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL", table, colName)
	// 	_, err = db.Exec(sql)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	duckColumns, duckdbColTypes, err := s.describeTable(table, db)
	if err != nil {
		return err
	}

	// input: json data, db column types, map[dbtype]jsontype
	log.Print(duckColumns)
	log.Print(duckdbColTypes)

	_, err = input.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	tempFile := s.S3Prefix + "/temp/" + ulid.Make().String() + ".ndjson"
	err = s.writeS3File(input, tempFile)
	if err != nil {
		return err
	}

	err = s.insertFromS3(table, tempFile, db)

	// Upload to s3
	// insert from s3

	// conn, err := connector.Connect(context.TODO())
	// if err != nil {
	// 	return err
	// }
	// appender, err := duckdb.NewAppenderFromConn(conn, "", table)
	// if err != nil {
	// 	return err
	// }

	// scanner := bufio.NewScanner(input)
	// maxCapacity := 100_000_000
	// buf := make([]byte, 2_000)
	// scanner.Buffer(buf, maxCapacity)

	// for scanner.Scan() {
	// 	log.Print("x")
	// 	parsed := gjson.ParseBytes(scanner.Bytes())
	// 	vals := make([]driver.Value, len(duckColumns))

	// 	for i, colName := range duckColumns {
	// 		duckDBColType := duckdbColTypes[colName]
	// 		vals[i] = s.jsonToGoType(duckDBColType, parsed.Get(colName))
	// 	}

	// 	// log.Trace().Interface("vals", vals).Int("row", row).Send()
	// 	log.Print(vals)
	// 	err = appender.AppendRowArray(vals)
	// 	log.Print(err)
	// 	if err != nil {
	// 		log.Error().Err(err).Bytes("data", scanner.Bytes()).Msg("Unable to add item to batch")
	// 		return err
	// 	}
	// }
	// err = appender.Flush()
	// log.Print(err)
	// err = appender.Close()
	// log.Print(err)
	// return err

	return nil
}
