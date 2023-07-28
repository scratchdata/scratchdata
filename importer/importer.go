package importer

import (
	"encoding/json"
	"log"
	"scratchdb/config"
	"sync"
	"time"

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

func (im *Importer) consumeMessages(pid int) {
	defer im.wg.Done()
	log.Println("Starting worker", pid)
	for message := range im.msgChan {
		log.Println(pid, message)
		time.Sleep(1 * time.Second)
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
