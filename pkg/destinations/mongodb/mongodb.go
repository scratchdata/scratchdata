package mongodb

import (
	"context"
	"encoding/json"
	"io"

	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBServer struct {
	URI      string `mapstructure:"uri"`
	Database string `mapstructure:"database"`

	db *mongo.Database
}

func (s *MongoDBServer) QueryNDJson(query string, writer io.Writer) error {
	coll := s.db.Collection("transactions")

	cursor, err := coll.Find(context.TODO(), bson.M{}, options.Find().SetLimit(2))
	if err != nil {
		return err
	}

	for cursor.Next(context.TODO()) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			return err
		}

		v, err := json.Marshal(result)
		if err != nil {
			return err
		}
		writer.Write(v)
		writer.Write([]byte("\n"))
	}

	return nil
}
func (s *MongoDBServer) QueryJSON(query string, writer io.Writer) error {
	// Create a buffered reader for efficient reading
	// ndjsonReader := strings.NewReader(ndjsonData)
	// reader := bufio.NewReader(ndjsonReader)

	// // Create a buffered writer for efficient writing
	// writer := bufio.NewWriter(jsonWriter)

	// query = `db.xy.find({"a":{}})`

	// tokens := strings.SplitN(query, ".", 3)
	// log.Print(tokens)
	// collection := tokens[1]

	// start := strings.Index(query, "(")
	// end := strings.LastIndex(query, ")")

	// jsonList := "[" + query[start+1:end] + "]"
	// log.Print(jsonList)

	// // isFind := strings.HasPrefix(tokens[2], "find(")
	// // isAggregate := strings.HasPrefix(tokens[2], "aggregate(")

	return nil
}
func (s *MongoDBServer) QueryCSV(query string, writer io.Writer) error { return nil }

func (s *MongoDBServer) Tables() ([]string, error)                     { return nil, nil }
func (s *MongoDBServer) Columns(table string) ([]models.Column, error) { return nil, nil }

func (s *MongoDBServer) CreateEmptyTable(name string) error                       { return nil }
func (s *MongoDBServer) CreateColumns(table string, filePath string) error        { return nil }
func (s *MongoDBServer) InsertFromNDJsonFile(table string, filePath string) error { return nil }

func (s *MongoDBServer) Close() error {
	return nil
}

func OpenServer(settings map[string]any) (*MongoDBServer, error) {
	srv := util.ConfigToStruct[MongoDBServer](settings)

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(srv.URI).SetServerAPIOptions(serverAPI)

	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		return nil, err
	}

	db := client.Database(srv.Database)

	srv.db = db

	return srv, nil
}
