package mongodb

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBServer struct {
}

func (s *MongoDBServer) QueryNDJson(query string, writer io.Writer) error { return nil }
func (s *MongoDBServer) QueryJSON(query string, writer io.Writer) error {
	query = `db.xy.find({"a":{}})`

	tokens := strings.SplitN(query, ".", 3)
	log.Print(tokens)
	collection := tokens[1]

	start := strings.Index(query, "(")
	end := strings.LastIndex(query, ")")

	jsonList := "[" + query[start+1:end] + "]"
	log.Print(jsonList)

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI("x").SetServerAPIOptions(serverAPI)
	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		return err
	}

	db := client.Database("x")
	// db.RunCommand()
	coll := db.Collection(collection)

	// coll.Run
	// coll.Find()

	cursor, _ := coll.Find(context.Background(), "")

	for cursor.Next(context.TODO()) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%+v\n", result)
	}

	// isFind := strings.HasPrefix(tokens[2], "find(")
	// isAggregate := strings.HasPrefix(tokens[2], "aggregate(")

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
	return srv, nil
}
