package duckdb

import (
	"context"
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
)

type DuckDBServer struct {
	Database string `mapstructure:"database"`
	Token    string `mapstructure:"token"`

	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Bucket          string `mapstructure:"bucket"`
	Region          string `mapstructure:"region"`
	S3Prefix        string `mapstructure:"s3_prefix"`
	Endpoint        string `mapstructure:"endpoint"`
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

func (s *DuckDBServer) getConnector() (driver.Connector, error) {

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
			_, err := execer.ExecContext(context.TODO(), qry, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return connector, err
}
