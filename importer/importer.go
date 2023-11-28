package importer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"scratchdb/apikeys"
	"scratchdb/chooser"
	"scratchdb/client"
	"scratchdb/config"
	"scratchdb/servers"
	"scratchdb/util"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spyzhov/ajson"
)

type Importer struct {
	Config *config.Config
	Client *client.Client

	wg            sync.WaitGroup
	msgChan       chan map[string]string
	done          chan bool
	apiKeys       apikeys.APIKeys
	serverManager servers.ClickhouseManager
	chooser       chooser.ServerChooser
}

func NewImporter(config *config.Config, apiKeyManager apikeys.APIKeys, serverManager servers.ClickhouseManager, chooser chooser.ServerChooser) *Importer {
	i := &Importer{
		Config:        config,
		Client:        client.NewClient(config),
		msgChan:       make(chan map[string]string),
		done:          make(chan bool),
		apiKeys:       apiKeyManager,
		serverManager: serverManager,
		chooser:       chooser,
	}
	return i
}

func (im *Importer) produceMessages() {
	defer im.wg.Done()
	log.Info().Msg("Starting producer")

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
			log.Error().Err(err).Msg("")
			continue
		}

		if len(msgResult.Messages) == 0 {
			log.Info().Msg("No messages from AWS, sleeping")
			time.Sleep(time.Duration(im.Config.Insert.SleepSeconds) * time.Second)
		}

		for _, message := range msgResult.Messages {

			// Ensure we haven't filled up disk
			// TODO: ensure we have enough disk space for: max file upload size, temporary file for insert statement, add'l overhead
			// Could farm this out to AWS batch with a machine sized for the data.
			currentFreeSpace := util.FreeDiskSpace(im.Config.Insert.DataDir)
			if currentFreeSpace <= uint64(im.Config.Insert.FreeSpaceRequiredBytes) {
				log.Error().Msg("Disk is full, not consuming any messages")
				time.Sleep(1 * time.Minute)
				continue
			}

			jsonMsg := *message.Body
			payload := map[string]string{}
			err = json.Unmarshal([]byte(jsonMsg), &payload)
			if err != nil {
				log.Error().Err(err).Stringer("message", message).Msg("Could not parse")
			} else {
				log.Info().Msg("Sending message to channel")
				_, err = sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      &im.Config.AWS.SQS,
					ReceiptHandle: message.ReceiptHandle,
				})
				if err != nil {
					log.Err(err).Msg("unable to retract message from queue")
				}
				im.msgChan <- payload
			}
		}
	}
}

func (im *Importer) createDB(conn driver.Conn, db string) error {
	sql := "CREATE DATABASE IF NOT EXISTS " + db + ";"
	err := conn.Exec(context.Background(), sql)
	return err
}

func (im *Importer) executeSQL(server servers.ClickhouseServer, sql string) error {
	conn, err := server.Connection()
	if err != nil {
		return err
	}
	err = conn.Exec(context.Background(), sql)
	return err
}

func (im *Importer) createTable(server servers.ClickhouseServer, user apikeys.APIKeyDetails, table string) error {
	sql := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS "%s"."%s"
	(
		__row_id String
	)
	ENGINE = MergeTree
	PRIMARY KEY(__row_id)
	`, user.GetDBName(), table)

	if server.GetStoragePolicy() != "" {
		sql += fmt.Sprintf("SETTINGS storage_policy='%s'", server.GetStoragePolicy())
	}

	return im.executeSQL(server, sql)
}

func (im *Importer) getColumnsLocal(fileName string) ([]string, error) {
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

	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		log.Err(err).Msg("query execution failure")
		return []string{}, err
	}

	for rows.Next() {
		var column string
		err := rows.Scan(&column)
		if err != nil {
			log.Err(err).Msgf("Unable to read columns %s %s", bucket, key)
			continue
		}
		colMap[column] = true
	}

	columns := make([]string, 0)
	for k := range colMap {
		columns = append(columns, k)
	}
	return columns, err
}

func (im *Importer) renameColumn(orig string) string {
	return strings.ReplaceAll(orig, ".", "_")
}

func (im *Importer) createColumns(server servers.ClickhouseServer, user apikeys.APIKeyDetails, table string, columns []string) error {
	sql := fmt.Sprintf(`ALTER TABLE "%s"."%s" `, user.GetDBName(), table)
	columnSql := make([]string, len(columns))
	for i, column := range columns {
		columnSql[i] = fmt.Sprintf(`ADD COLUMN IF NOT EXISTS "%s" String`, im.renameColumn(column))
	}

	sql += strings.Join(columnSql, ", ")
	return im.executeSQL(server, sql)
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

func (im *Importer) insertDataLocal(server servers.ClickhouseServer, user apikeys.APIKeyDetails, localFile, table string, columns []string) error {
	insertSql := fmt.Sprintf(`INSERT INTO "%s"."%s" (`, user.GetDBName(), table)

	for i, column := range columns {
		insertSql += fmt.Sprintf("`%s`", im.renameColumn(column))
		if i < len(columns)-1 {
			insertSql += ","
		}
	}
	insertSql += ")"

	conn, err := server.Connection()
	if err != nil {
		return err
	}

	batch, err := conn.PrepareBatch(context.Background(), insertSql)
	if err != nil {
		log.Err(err).Msg("unable to initiate batch query")
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
			log.Err(err).Msg("error parsing json")
			return err
		}

		nodes, err := data.JSONPath("$")
		for _, node := range nodes {
			vals := make([]interface{}, len(columns))
			for i, c := range columns {
				v, err := node.GetKey(c)
				if err != nil {
					vals[i] = ""
				} else {
					if v.IsString() {
						vals[i], err = strconv.Unquote(v.String())
						if err != nil {
							batch.Abort()
							return err
						}
					} else {
						vals[i] = v.String()
					}
				}
			}
			batch.Append(vals...)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Err(err).Msg("scanner error")
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

func (im *Importer) consumeMessages(pid int) {
	defer im.wg.Done()
	defer log.Info().Msgf("Stopping worker %d", pid)
	log.Info().Msgf("Starting worker %d", pid)

	// TODO: figure out where this should live
	// defer func(conn driver.Conn) {
	// 	err := conn.Close()
	// 	if err != nil {
	// 		log.Println("failed to properly close connection")
	// 	}
	// }(conn)

	for message := range im.msgChan {
		log.Debug().Msgf("%#v", message)
		api_key := message["api_key"]
		table := message["table_name"]
		bucket := message["bucket"]
		key := message["key"]

		log.Debug().Msgf("%s %s %s %s", api_key, table, bucket, key)

		if api_key == "" || table == "" {
			tokens := strings.Split(key, "/")
			lastTok := len(tokens) - 1
			table = tokens[lastTok-1]
			api_key = tokens[lastTok-2]
			log.Debug().Msgf("%s %s %s %s", api_key, table, bucket, key)
		}

		keyDetails, ok := im.apiKeys.GetDetailsByKey(api_key)

		if !ok {
			log.Info().Msgf("Discarding unknown user, api key: %s, %s", api_key, key)
			continue
		}

		log.Debug().Msgf("Starting to import %s", key)

		server, err := im.chooser.ChooseServerForWriting(im.serverManager, keyDetails)
		if err != nil {
			log.Err(err).Msgf("Unable to choose server for %s", keyDetails.GetName())
			log.Info().Msgf("Did not process message %s", key)
			continue
		}

		// download file locally with url path
		// delete file if there's an error
		// add file/message info to debug log
		// requeue message depending on if it is recoverable (bad json vs ch full)

		log.Debug().Msgf("Downloading file %s", key)
		localPath, err := im.downloadFile(bucket, key)
		if err != nil {
			log.Err(err).Msgf("Unable to download file %s", key)
			continue
		}

		log.Debug().Msgf("Creating table %s", key)
		// 2. Create table if not exists, give a default pk of a row id which is a ulid
		err = im.createTable(server, keyDetails, table)
		if err != nil {
			log.Err(err).Msgf("Unable to create table %s", key)
			continue
		}

		// 3. Get a list of columns from the json
		log.Debug().Msgf("Getting columns %s", key)
		columns, err := im.getColumnsLocal(localPath)
		// columns, err := im.getColumns(conn, bucket, key)
		if err != nil {
			log.Err(err).Msg("failed to retrieve columns")
			continue
		}

		// 4. Alter table to create columns
		log.Debug().Msgf("Creating columns %s", key)
		err = im.createColumns(server, keyDetails, table, columns)
		if err != nil {
			log.Err(err).Msgf("failed to create columns")
			continue
		}
		// 5. Import json data
		log.Debug().Msgf("Inserting data %s", key)
		err = im.insertDataLocal(server, keyDetails, localPath, table, columns)
		// err = im.insertData(conn, bucket, key, user, table, columns)
		if err != nil {
			log.Err(err).Msg("")
			continue
		}

		log.Debug().Msgf("Deleting local data post-insert %s", key)
		err = os.Remove(localPath)
		if err != nil {
			log.Err(err).Msgf("unable to delete file locally %s", key)
		}

		log.Debug().Msgf("Done importing %s", key)
	}
}

func (im *Importer) Start() {
	log.Info().Msg("Starting Importer")

	err := os.MkdirAll(im.Config.Insert.DataDir, os.ModePerm)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to make required directories")
	}

	err = im.apiKeys.Healthy()
	if err != nil {
		log.Fatal().Err(err).Msg("api keys are unhealthy")
	}

	im.wg.Add(1)
	go im.produceMessages()

	im.wg.Add(im.Config.Insert.Workers)
	for i := 0; i < im.Config.Insert.Workers; i++ {
		go im.consumeMessages(i)
	}
}

func (im *Importer) Stop() error {
	log.Info().Msg("Shutting down Importer")
	im.done <- true
	im.wg.Wait()
	return nil
}
