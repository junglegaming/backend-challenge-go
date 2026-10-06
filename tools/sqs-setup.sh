#!/bin/bash
awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true
awslocal sqs create-queue --queue-name outbox-events.fifo --attributes FifoQueue=true,ContentBasedDeduplication=true
