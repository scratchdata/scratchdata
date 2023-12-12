package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"scratchdata/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
)

type DuckDBServer struct {
	Database string `mapstructure:"database"`
	Token    string `mapstructure:"token"`

	// type Storage struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	S3Bucket        string `mapstructure:"s3_bucket"`
	Region          string `mapstructure:"region"`
	Endpoint        string `mapstructure:"endpoint"`
	// }

	s3Client *s3.S3
}

var jsonToDuck = map[string]string{
	"string": "STRING",
	"int":    "BIGINT",
	"float":  "DOUBLE",
	"bool":   "BOOLEAN",
}

// func (s *DuckDBServer) jsonToDuckDBType(duckDbType string, data gjson.Result) any {
// 	switch duckDbType {
// 	case "BIGINT":
// 	case "BIT":
// 	case "BOOLEAN":
// 	case "BLOB":
// 	case "DATE":
// 	case "DOUBLE":
// 	case "INTEGER":
// 	case "DECIMAL":// TODO
// 	// case "String", "FixedString":
// 	// 	return data.String()
// 	// case "Decimal":
// 	// 	return decimal.NewFromFloat(data.Float())
// 	// case "Bool":
// 	// 	return data.Bool()
// 	// case "UInt8":
// 	// 	return uint8(data.Uint())
// 	// case "UInt16":
// 	// 	return uint16(data.Uint())
// 	// case "UInt32":
// 	// 	return uint32(data.Uint())
// 	// case "UInt64":
// 	// 	return data.Uint()
// 	// case "UInt128", "UInt256":
// 	// 	n := new(big.Int)
// 	// 	n.SetString(data.String(), 10)
// 	// 	return n
// 	// case "Int8":
// 	// 	return int8(data.Int())
// 	// case "Int16":
// 	// 	return int16(data.Int())
// 	// case "Int32":
// 	// 	return int32(data.Int())
// 	// case "Int64":
// 	// 	return data.Int()
// 	// case "Int128", "Int256":
// 	// 	n := new(big.Int)
// 	// 	n.SetString(data.String(), 10)
// 	// 	return n
// 	// case "Float32":
// 	// 	return float32(data.Float())
// 	// case "Float64":
// 	// 	return data.Float()
// 	// case "UUID":
// 	// 	return data.String()
// 	// case "Date", "Date32":
// 	// 	return data.String()
// 	// case "DateTime", "DateTime64":
// 	// 	if data.Type == gjson.Number {
// 	// 		return data.Int()
// 	// 	} else {
// 	// 		return data.String()
// 	// 	}
// 	// case "Enum8":
// 	// 	return int8(data.Int())
// 	// case "Enum16":
// 	// 	return int16(data.Int())
// 	}

// 	return data.String()
// }

func (s *DuckDBServer) getS3Client() (*s3.S3, error) {
	storageCreds := credentials.NewStaticCredentials(s.AccessKeyId, s.SecretAccessKey, "")
	storageConfig := aws.NewConfig().
		WithRegion(s.Region).
		WithCredentials(storageCreds).
		WithS3ForcePathStyle(true)

	if s.Endpoint != "" {
		storageConfig.WithEndpoint(s.Endpoint)
	}

	session, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	return s3.New(session, storageConfig), nil
}

func (s *DuckDBServer) writeS3File(input io.ReadSeeker, destination string) error {
	s3Client, err := s.getS3Client()
	if err != nil {
		return err
	}

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.S3Bucket),
		Key:    aws.String(destination),
		Body:   input,
	})

	return err
}

func (s *DuckDBServer) InsertBatchFromNDJson(input io.ReadSeeker) error {
	table := "logs"

	jsonTypes, err := util.GetJSONTypes(input)
	if err != nil {
		return err
	}

	connector, err := duckdb.NewConnector("md:"+s.Database+"?motherduck_token="+s.Token, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'json'",
			"LOAD 'json'",
			"INSTALL 'aws'",
			"LOAD 'aws'",
			"INSTALL 'httpfs'",
			"LOAD 'httpfs'",
		}

		for _, qry := range bootQueries {
			_, err = execer.ExecContext(context.TODO(), qry, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
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

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (__row_id STRING)", table)
	_, err = db.Exec(sql)
	if err != nil {
		return err
	}

	for colName, jsonType := range jsonTypes {
		sql = fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table, colName, jsonToDuck[jsonType])
		_, err = db.Exec(sql)
		if err != nil {
			return err
		}

		// sql = fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL", table, colName)
		// _, err = db.Exec(sql)
		// if err != nil {
		// 	return err
		// }
	}

	sql = fmt.Sprintf("DESCRIBE \"%s\"", table)
	rows, err := db.Query(sql)
	if err != nil {
		return err
	}
	duckdbColTypes := make(map[string]string)

	// describeCols, err := rows.Columns()
	// describeVals := make([]*any, len(describeCols))

	duckColumns := []string{}

	for rows.Next() {
		var colName, colType string
		var isNull, key, defaultValue, extra *string
		if err := rows.Scan(&colName, &colType, &isNull, &key, &defaultValue, &extra); err != nil {
			return err
		}

		duckdbColTypes[colName] = colType
		duckColumns = append(duckColumns, colName)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// input: json data, db column types, map[dbtype]jsontype
	log.Print(duckColumns)
	log.Print(duckdbColTypes)

	_, err = input.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	tempFile := "temp/" + ulid.Make().String() + ".ndjson"
	s.writeS3File(input, tempFile)

	sql = fmt.Sprintf(`
		INSERT INTO "%s" 
		SELECT * FROM
		read_ndjson_auto(
			's3://%s/%s?s3_region=%s&s3_access_key_id=%s&s3_secret_access_key=%s&s3_endpoint=%s&s3_use_ssl=true'
		 )
		`,
		table, s.S3Bucket, tempFile, s.Region, s.AccessKeyId, s.SecretAccessKey, s.Endpoint,
	)

	log.Print(sql)
	/*
		SELECT * FROM 's3://bucket/file.parquet?s3_region=region&s3_session_token=session_token' T1

		copy t from 's3://bucket/file.parquet?s3_region=region&s3_access_key_id=session_token&s3_secret_access_key=x&s3_endpoint=x&s3_use_ssl=true'

	*/

	// s.deleteS3File(tempFile)

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

func (s *DuckDBServer) QueryJSON(query string, writer io.Writer) error {
	sanitized := util.TrimQuery(query)

	db, err := sql.Open("duckdb", "md:"+s.Database+"?motherduck_token="+s.Token)
	if err != nil {
		return err
	}

	defer db.Close()

	db.Query("INSTALL 'json'")
	db.Query("LOAD 'json'")

	rows, err := db.Query("DESCRIBE " + sanitized)
	if err != nil {
		return err
	}

	var columnName string
	var columnType *string
	var null *string
	var key *string
	var defaultVal *interface{}
	var extra *string
	columnNames := make([]string, 0)

	for rows.Next() {
		err := rows.Scan(&columnName, &columnType, &null, &key, &defaultVal, &extra)
		if err != nil {
			return err
		}
		columnNames = append(columnNames, columnName)
	}

	rows.Close()

	rows, err = db.Query("SELECT to_json(COLUMNS(*)) FROM (" + sanitized + ")")
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	writer.Write([]byte("["))

	// https://groups.google.com/g/golang-nuts/c/-9h9UwrsX7Q
	pointers := make([]interface{}, len(cols))
	container := make([]*string, len(cols))

	for i, _ := range pointers {
		pointers[i] = &container[i]
	}

	hasNext := rows.Next()
	for hasNext {
		err := rows.Scan(pointers...)
		if err != nil {
			return err
		}

		writer.Write([]byte("{"))
		for i, _ := range cols {
			writer.Write([]byte("\""))
			writer.Write([]byte(util.JsonEscape(columnNames[i])))
			writer.Write([]byte("\""))

			writer.Write([]byte(":"))

			if container[i] == nil {
				writer.Write([]byte("null"))
			} else {
				writer.Write([]byte(*container[i]))
			}

			if i < len(cols)-1 {
				writer.Write([]byte(","))
			}
		}

		writer.Write([]byte("}"))

		hasNext = rows.Next()

		if hasNext {
			writer.Write([]byte(","))
		}
	}

	writer.Write([]byte("]"))

	return nil
}
