#!/bin/bash

set -e

export AWS_ACCESS_KEY_ID=localstack
export AWS_SECRET_ACCESS_KEY=localstack
export AWS_DEFAULT_REGION=us-east-1

echo "Create S3 bucket"
aws --endpoint-url=http://127.0.0.1:4566 s3api create-bucket --bucket scratch

echo "Create SQS"
aws --endpoint-url=http://127.0.0.1:4566 sqs create-queue --queue-name scratchq
