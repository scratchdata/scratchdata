package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	apikeys "scratchdb/api_keys"
	"scratchdb/client"
	"scratchdb/config"
	"scratchdb/util"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/spyzhov/ajson"
)

type Importer struct {
	Config *config.Config
	Client *client.Client

	wg      sync.WaitGroup
	msgChan chan map[string]string
	done    chan bool
	apiKeys apikeys.APIKeys
}

func NewImporter(config *config.Config, apiKeyManager apikeys.APIKeys) *Importer {
	i := &Importer{
		Config:  config,
		Client:  client.NewClient(config),
		msgChan: make(chan map[string]string),
		done:    make(chan bool),
		apiKeys: apiKeyManager,
	}
	return i
}

func (im *Importer) produceMessages() {
	defer im.wg.Done()
	log.Println("Starting producer")

	sqsClient := im.Client.SQS

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
			WaitTimeSeconds:     aws.Int64(20),
		})

		if err != nil {
			log.Println(err)
			continue
		}

		if len(msgResult.Messages) == 0 {
			log.Println("No messages from AWS, sleeping")
			time.Sleep(time.Duration(im.Config.Insert.SleepSeconds) * time.Second)
		}

		for _, message := range msgResult.Messages {

			// Ensure we haven't filled up disk
			// TODO: ensure we have enough disk space for: max file upload size, temporary file for insert statement, add'l overhead
			// Could farm this out to AWS batch with a machine sized for the data.
			currentFreeSpace := util.FreeDiskSpace(im.Config.Insert.DataDir)
			if currentFreeSpace <= uint64(im.Config.Insert.FreeSpaceRequiredBytes) {
				log.Println("Disk is full, not consuming any messages")
				time.Sleep(1 * time.Minute)
				continue
			}

			jsonMsg := *message.Body
			payload := map[string]string{}
			err = json.Unmarshal([]byte(jsonMsg), &payload)
			if err != nil {
				log.Println("Could not parse", message, err)
			} else {
				log.Println("Sending message to channel")
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
	clickhouseServer := im.Config.Clickhouse.ID
	serverConfig, ok := im.Config.ClickhouseServers[clickhouseServer]

	storagePolicy := "default"
	if ok && serverConfig.StoragePolicy != "" {
		storagePolicy = serverConfig.StoragePolicy
	}

	sql := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS "%s"."%s"
	(
		__row_id String
	)
	ENGINE = MergeTree
	PRIMARY KEY(__row_id)
	SETTINGS storage_policy='%s';
	`, db, table, storagePolicy)
	err := conn.Exec(context.Background(), sql)
	return err
}

func (im *Importer) getColumnsLocal(conn driver.Conn, fileName string) ([]string, error) {
	keys := make(map[string]bool)
	rc := make([]string, 0)
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
		data, err := ajson.Unmarshal([]byte(scanner.Text()))
		if err != nil {
			return rc, err
		}

		nodes, err := data.JSONPath("$")
		for _, node := range nodes {
			for _, key := range node.Keys() {
				keys[key] = true
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return rc, err
	}

	for k := range keys {
		rc = append(rc, k)
	}
	return rc, nil
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
	sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" `, db, table)
	columnSql := make([]string, len(columns))
	for i, column := range columns {
		columnSql[i] = fmt.Sprintf(`ADD COLUMN IF NOT EXISTS "%s" String`, im.renameColumn(column))
	}

	sql += strings.Join(columnSql, ", ")
	err := conn.Exec(context.Background(), sql)
	if err != nil {
		return err
	}
	return nil
}

func (im *Importer) downloadFile(bucket, key string) (string, error) {
	filename := filepath.Base(key)
	localPath := filepath.Join(im.Config.Insert.DataDir, filename)

	file, err := os.Create(localPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	downloader := s3manager.NewDownloaderWithClient(im.Client.S3)
	_, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return "", err
	}

	return localPath, nil
}

func (im *Importer) insertDataLocal(conn driver.Conn, localFile, db, table string, columns []string) error {
	insertSql := fmt.Sprintf(`INSERT INTO "%s"."%s" (`, db, table)

	insertSql += "`__row_id` , "
	for i, column := range columns {
		insertSql += fmt.Sprintf("`%s`", im.renameColumn(column))
		if i < len(columns)-1 {
			insertSql += ","
		}
	}
	insertSql += ")"

	batch, err := conn.PrepareBatch(context.Background(), insertSql)
	if err != nil {
		log.Println(err)
		return err
	}

	file, err := os.Open(localFile)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	maxCapacity := 100_000_000
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {

		data, err := ajson.Unmarshal([]byte(scanner.Text()))
		if err != nil {
			batch.Abort()
			log.Println(err)
			return err
		}

		nodes, err := data.JSONPath("$")
		for _, node := range nodes {
			vals := make([]interface{}, len(columns)+1)
			vals[0] = ulid.Make().String()
			for i, c := range columns {
				v, err := node.GetKey(c)
				if err != nil {
					vals[i+1] = ""
				} else {
					if v.IsString() {
						vals[i+1], err = strconv.Unquote(v.String())
						if err != nil {
							batch.Abort()
							return err
						}
					} else {
						vals[i+1] = v.String()
					}
				}
			}
			batch.Append(vals...)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Println(err)
		batch.Abort()
		return err
	}

	return batch.Send()
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
			Debug:           false,
			MaxOpenConns:    im.Config.Insert.MaxOpenConns,
			MaxIdleConns:    im.Config.Insert.MaxIdleConns,
			ConnMaxLifetime: time.Second * time.Duration(im.Config.Insert.ConnMaxLifetimeSecs),
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
	defer log.Println("Stopping worker", pid)
	log.Println("Starting worker", pid)

	conn, err := im.connect()
	if err != nil {
		panic(errors.Wrap(err, "unable to connect to clickhouse"))
	}
	defer func(conn driver.Conn) {
		err := conn.Close()
		if err != nil {
			log.Println("failed to properly close connection")
		}
	}(conn)

	for message := range im.msgChan {
		log.Println(message)
		api_key := message["api_key"]
		table := message["table_name"]
		bucket := message["bucket"]
		key := message["key"]

		log.Println(api_key, table, bucket, key)

		if api_key == "" || table == "" {
			tokens := strings.Split(key, "/")
			lastTok := len(tokens) - 1
			table = tokens[lastTok-1]
			api_key = tokens[lastTok-2]
			log.Println(api_key, table, bucket, key)
		}

		keyDetails, ok := im.apiKeys.GetDetailsByKey(api_key)
		if !ok {
			log.Println("Discarding unknown user, api key", api_key, key)
			continue
		}
		user := keyDetails.GetDBUser()

		if user == "" {
			log.Println("Discarding unknown user, api key", api_key, key)
			continue
		}

		log.Println("Starting to import", key)
		// 1. Create DB if not exists
		err = im.createDB(conn, user)
		if err != nil {
			log.Println(err)
			continue
		}

		// download file locally with url path
		// delete file if there's an error
		// add file/message info to debug log
		// requeue message depending on if it is recoverable (bad json vs ch full)

		log.Println("Downloading file", key)
		localPath, err := im.downloadFile(bucket, key)
		if err != nil {
			log.Println("Unable to download file", key, err)
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
		columns, err := im.getColumnsLocal(conn, localPath)
		// columns, err := im.getColumns(conn, bucket, key)
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
		err = im.insertDataLocal(conn, localPath, user, table, columns)
		// err = im.insertData(conn, bucket, key, user, table, columns)
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("Deleting local data post-insert", key)
		err = os.Remove(localPath)
		if err != nil {
			log.Println("Unable to delete file locally", key)
		}

		log.Println("Done importing", key)
	}
}

func (im *Importer) Start() {
	log.Println("Starting Importer")

	err := os.MkdirAll(im.Config.Insert.DataDir, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	err = im.apiKeys.Healthy()
	if err != nil {
		log.Fatal(err)
	}

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
