#!/bin/sh
export REDIS_DB="redis://vCbdWwjecp@localhost:6379/0"
export REDIS_KEY="generate_ids"
export RABBIT="amqp://user:cAz7aCkLDotl0yRK@localhost:5672"
export ID="results_queue"
export KAFKA="localhost:9092"
export KAFKA_TOPIC="queue2"
export HTTP_PORT="8080"
export GIN_MODE=release

./service2

