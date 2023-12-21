package sqs

import (
	"fmt"
	"scratchdata/config"
	"scratchdata/pkg/queue"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/rs/zerolog/log"
)

// Queue implements queue.QueueBackend using SQS
type Queue struct {
	client *sqs.SQS
	sqsURL *string
}

// Enqueue implements queue.QueueBackend.Enqueue
func (q *Queue) Enqueue(message []byte) error {
	msg := string(message)
	_, err := q.client.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    q.sqsURL,
		MessageBody: aws.String(msg),
	})
	log.Debug().Str("sqs_url", *q.sqsURL).Err(err).Str("message", msg).Msg("Enqueue")
	if err != nil {
		return fmt.Errorf("Queue.Enqueue: %s: %w", *q.sqsURL, err)
	}
	return nil
}

// receive fetches a message from SQS
//
// If no valid message is received, error queue.ErrEmpyQueue is returned
func (q *Queue) receive() (*sqs.Message, error) {
	res, err := q.client.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            q.sqsURL,
		MaxNumberOfMessages: aws.Int64(1),
	})
	if err != nil {
		return nil, err
	}
	for _, msg := range res.Messages {
		if msg.Body != nil {
			return msg, nil
		}
	}
	return nil, queue.ErrEmpyQueue
}

// Dequeue implememnts queue.QueueBackend.Dequeue
func (q *Queue) Dequeue() ([]byte, error) {
	msg, err := q.receive()
	log.Debug().Str("sqs_url", *q.sqsURL).Err(err).Any("message", msg).Msg("Dequeue")
	if err != nil {
		return nil, fmt.Errorf("Queue.Dequeue: %s: %w", *q.sqsURL, err)
	}

	_, err = q.client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      q.sqsURL,
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		log.Error().Err(err).Msg("Unable to delete message from SQS")
	}

	return []byte(*msg.Body), nil
}

// NewQueue returns a new initialized Queue
func NewQueue(c config.SQS) *Queue {
	awsConfig := aws.NewConfig().
		WithRegion(c.Region).
		WithCredentials(credentials.NewStaticCredentials(c.AccessKeyId, c.SecretAccessKey, ""))

	if c.Endpoint != "" {
		awsConfig.WithEndpoint(c.Endpoint)
	}

	q := &Queue{
		client: sqs.New(session.Must(session.NewSession()), awsConfig),
		sqsURL: aws.String(c.SqsURL),
	}
	return q
}
