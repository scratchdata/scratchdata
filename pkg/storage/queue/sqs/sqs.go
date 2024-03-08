package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/scratchdata/scratchdata/util"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/rs/zerolog/log"
)

// Queue implements queue.QueueBackend using SQS
type Queue struct {
	URL             string `mapstructure:"url"`
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Region          string `mapstructure:"region"`

	client *sqs.Client
}

// Enqueue implements queue.QueueBackend.Enqueue
func (q *Queue) Enqueue(message []byte) error {
	msg := string(message)
	_, err := q.client.SendMessage(context.TODO(), &sqs.SendMessageInput{
		QueueUrl:    aws.String(q.URL),
		MessageBody: aws.String(msg),
	})
	log.Trace().Str("sqs_url", q.URL).Err(err).Str("message", msg).Msg("Enqueue")
	if err != nil {
		return err
	}
	return nil
}

// receive fetches a message from SQS
//
// If no valid message is received, error queue.ErrEmpyQueue is returned
func (q *Queue) receive() (types.Message, bool) {
	res, err := q.client.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.URL),
		MaxNumberOfMessages: 1,
	})
	if err != nil {
		log.Error().Err(err).Msg("Unable to poll SQS")
		return types.Message{}, false
	}
	for _, msg := range res.Messages {
		if msg.Body != nil {
			return msg, true
		}
	}
	return types.Message{}, false
}

func (q *Queue) delete(receiptHandle *string) error {
	_, err := q.client.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.URL),
		ReceiptHandle: receiptHandle,
	})
	return err
}

// Dequeue implememnts queue.QueueBackend.Dequeue
func (q *Queue) Dequeue() ([]byte, bool) {
	msg, ok := q.receive()
	if !ok {
		return nil, ok
	}

	err := q.delete(msg.ReceiptHandle)
	if err != nil {
		log.Error().Err(err).Str("sqs_receipt_handle", *msg.ReceiptHandle).Str("message", *msg.Body).Msg("Unable to delete message from SQS")
	}

	return []byte(*msg.Body), true
}

// NewQueue returns a new initialized Queue
func NewQueue(c map[string]any) (*Queue, error) {
	q := util.ConfigToStruct[Queue](c)

	appCreds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(q.AccessKeyId, q.SecretAccessKey, ""))
	//value, err := appCreds.Retrieve(context.TODO())
	//if err != nil {
	//	return nil, err
	//}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.Region = "us-east-1"
		o.Credentials = appCreds
	})

	q.client = client

	return q, nil
}
