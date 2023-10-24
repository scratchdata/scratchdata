package ingest

import (
	"encoding/json"
	"io"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gofiber/fiber/v2"
	"github.com/spyzhov/ajson"
	"scratchdb/client"
)

// uploadToS3 uploads file to S3 bucket using key
func uploadToS3(c *client.Client, file io.ReadSeeker, bucket, key string) (err error) {
	_, err = c.S3.PutObject(&s3.PutObjectInput{
		Bucket:             &bucket,
		Key:                &key,
		Body:               file,
		ContentDisposition: aws.String("attachment"),
	})
	return
}

// addToSQS sends a message to an SQS queue.
// It returns an error if there's any failure in marshaling
// the message to JSON or in sending the message to the SQS queue.
func addToSQS(c *client.Client, queue string, msg any) error {

	sqsPayload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	log.Println("SQS JSON Payload", string(sqsPayload))

	_, err = c.SQS.SendMessage(
		&sqs.SendMessageInput{
			MessageBody: aws.String(string(sqsPayload)),
			QueueUrl:    &queue,
		})
	if err != nil {
		return err
	}
	return nil
}

// handleFileUpload writes data to S3 bucket
// and consequently sends a message to the SQS queueName
func handleFileUpload(
	c *client.Client, data io.ReadSeeker,
	bucket, key string,
	queueName string, msgData any) error {
	err := uploadToS3(c, data, bucket, key)
	if err != nil {
		log.Printf("failed to upload parquet to S3: %s\n", err)
		return err
	}
	err = addToSQS(c, queueName, msgData)
	if err != nil {
		log.Printf("failed to push parquet to SQS: %s\n", err)
		return err
	}
	return nil
}

func handleJSONUpload(input []byte, writer *FileWriter, dataPath, flattenAlgo string) error {
	// Ensure JSON is valid
	if !json.Valid(input) {
		return fiber.ErrBadRequest
	}

	root, err := ajson.Unmarshal(input)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}

	x, err := root.JSONPath(dataPath)
	if err != nil {
		return err
	}

	node := x[0]
	switch node.Type() {
	case ajson.Array:
		err = flattenArray(flattenAlgo, node, writer)
	case ajson.Object:
		err = flattenObject(flattenAlgo, node, writer)
	}
	if err != nil {
		return err
	}
	return nil
}
