package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"scratchdb/config"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Importer struct {
	Config config.Config

	wg      sync.WaitGroup
	msgChan chan map[string]string
	done    chan bool
}

func NewImporter(config config.Config) *Importer {
	i := &Importer{
		Config:  config,
		msgChan: make(chan map[string]string),
		done:    make(chan bool),
	}
	return i
}

func (im *Importer) produceMessages() {
	defer im.wg.Done()

	creds := credentials.NewStaticCredentials(im.Config.AWS.AccessKeyId, im.Config.AWS.SecretAccessKey, "")
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(im.Config.AWS.Region),
		Credentials: creds,
	})
	if err != nil {
		log.Println(err)
		close(im.msgChan)
		return
	}

	sqsClient := sqs.New(sess)

	for {
		select {
		case <-im.done:
			close(im.msgChan)
			return
		default:
		}

		msgResult, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            &im.Config.AWS.SQS,
			MaxNumberOfMessages: aws.Int64(1),
			WaitTimeSeconds:     aws.Int64(1),
		})

		if err != nil {
			log.Println(err)
			continue
		}

		if len(msgResult.Messages) == 0 {
			// log.Println("No messages from AWS, sleeping")
			time.Sleep(time.Duration(im.Config.Insert.SleepSeconds) * time.Second)
		}

		for _, message := range msgResult.Messages {
			jsonMsg := *message.Body
			payload := map[string]string{}
			err = json.Unmarshal([]byte(jsonMsg), &payload)
			if err != nil {
				log.Println("Could not parse", message, err)
			} else {
				_, err = sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      &im.Config.AWS.SQS,
					ReceiptHandle: message.ReceiptHandle,
				})
				if err != nil {
					log.Println(err)
				}
				im.msgChan <- payload
			}
		}
	}
}

func (im *Importer) createCurl(sql string) string {
	log.Println(sql)
	curl := fmt.Sprintf("cat query.sql | curl '%s://%s:%s@%s:%s' -d @-",
		im.Config.Clickhouse.Protocol,
		im.Config.Clickhouse.Username,
		im.Config.Clickhouse.Password,
		im.Config.Clickhouse.Host,
		im.Config.Clickhouse.HTTPPort,
	)
	return curl
}

func (im *Importer) createDB(conn driver.Conn, db string) error {
	sql := "CREATE DATABASE IF NOT EXISTS " + db + ";"
	err := conn.Exec(context.Background(), sql)
	return err
}

func (im *Importer) createTable(conn driver.Conn, db string, table string) error {
	sql := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS "%s"."%s"
	(
		__row_id String
	)
	ENGINE = MergeTree
	PRIMARY KEY(__row_id);
	`, db, table)
	err := conn.Exec(context.Background(), sql)
	return err
}

func (im *Importer) getColumns(conn driver.Conn, bucket string, key string) ([]string, error) {
	colMap := make(map[string]bool)

	sql := fmt.Sprintf(`
		select 
			arrayJoin(JSONExtractKeys(COALESCE(c1,''))) as c
		from
			s3('https://%s.s3.amazonaws.com/%s','%s','%s', 'TabSeparatedRaw')`,
		bucket, key, im.Config.AWS.AccessKeyId, im.Config.AWS.SecretAccessKey)

	// log.Println(sql)

	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		log.Println(err)
		return []string{}, err
	}

	for rows.Next() {
		var column string
		err := rows.Scan(&column)
		if err != nil {
			log.Println("Unable to read columns", bucket, key, err)
			continue
		}
		colMap[column] = true
	}

	columns := make([]string, 0)
	for k, _ := range colMap {
		columns = append(columns, k)
	}
	return columns, err
}

func (im *Importer) renameColumn(orig string) string {
	return strings.ReplaceAll(orig, ".", "_")
}

func (im *Importer) createColumns(conn driver.Conn, db string, table string, columns []string) error {
	for _, column := range columns {
		sql := fmt.Sprintf(`
			ALTER TABLE "%s"."%s"
			ADD COLUMN IF NOT EXISTS
			"%s" String
			`, db, table, im.renameColumn(column))
		err := conn.Exec(context.Background(), sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func (im *Importer) insertData(conn driver.Conn, bucket, key, db, table string, columns []string) error {
	if len(columns) == 0 {
		return nil
	}

	sql := fmt.Sprintf(`INSERT INTO "%s"."%s" (`, db, table)

	sql += "__row_id , "

	for i, column := range columns {
		sql += fmt.Sprintf("\"%s\"", im.renameColumn(column))
		if i < len(columns)-1 {
			sql += ","
		}
	}

	sql += ") "
	sql += " SELECT "
	sql += " generateULID() as __row_id, "

	for i, column := range columns {
		sql += fmt.Sprintf("JSONExtractString(c1, '%s') AS \"%s\"", column, im.renameColumn(column))
		if i < len(columns)-1 {
			sql += ","
		}
	}
	sql += " FROM "
	sql += fmt.Sprintf("s3('https://%s.s3.amazonaws.com/%s','%s','%s', 'TabSeparatedRaw')", bucket, key, im.Config.AWS.AccessKeyId, im.Config.AWS.SecretAccessKey)

	err := conn.Exec(context.Background(), sql)

	return err
}

func (im *Importer) connect() (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%s", im.Config.Clickhouse.Host, im.Config.Clickhouse.TCPPort)},
			Auth: clickhouse.Auth{
				// Database: "default",
				Username: im.Config.Clickhouse.Username,
				Password: im.Config.Clickhouse.Password,
			},
			// ClientInfo: clickhouse.ClientInfo{
			// 	Products: []struct {
			// 		Name    string
			// 		Version string
			// 	}{
			// 		{Name: "scratchdb", Version: "1"},
			// 	},
			// },

			// Debugf: func(format string, v ...interface{}) {
			// 	fmt.Printf(format, v)
			// },
			// TLS: &tls.Config{
			// 	InsecureSkipVerify: true,
			// },
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

func (im *Importer) consumeMessages(pid int) {
	defer im.wg.Done()
	log.Println("Starting worker", pid)
	for message := range im.msgChan {
		// log.Println(message)
		api_key := message["api_key"]
		table := message["table_name"]
		bucket := message["bucket"]
		key := message["key"]

		if api_key == "" || table == "" {
			tokens := strings.Split(key, "/")
			lastTok := len(tokens) - 1
			table = tokens[lastTok-1]
			api_key = tokens[lastTok-2]
		}
		user := im.Config.Users[api_key]

		if user == "" {
			continue
		}

		conn, err := im.connect()
		if err != nil {
			continue
		}

		log.Println("Starting to import", key)
		// 1. Create DB if not exists
		err = im.createDB(conn, user)
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("Creating table", key)
		// 2. Create table if not exists, give a default pk of a row id which is a ulid
		err = im.createTable(conn, user, table)
		if err != nil {
			log.Println(err)
			continue
		}

		// 3. Get a list of columns from the json
		log.Println("Getting columns", key)
		columns, err := im.getColumns(conn, bucket, key)
		if err != nil {
			log.Println(err)
			continue
		}

		// 4. Alter table to create columns
		log.Println("Creating columnms", key)
		err = im.createColumns(conn, user, table, columns)
		if err != nil {
			log.Println(err)
			continue
		}
		// 5. Import json data
		log.Println("Inserting data", key)
		err = im.insertData(conn, bucket, key, user, table, columns)
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("Done importing", key)
	}
}

func (im *Importer) Start() {
	log.Println("Starting Importer")

	im.wg.Add(1)
	go im.produceMessages()

	im.wg.Add(im.Config.Insert.Workers)
	for i := 0; i < im.Config.Insert.Workers; i++ {
		go im.consumeMessages(i)
	}
}

func (im *Importer) Stop() error {
	log.Println("Shutting down Importer")
	im.done <- true
	im.wg.Wait()
	return nil
}
