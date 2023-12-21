#!/bin/sh
set -e

awslocal sqs create-queue --queue-name $TEST_QUEUE_NAME
