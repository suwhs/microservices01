#!/bin/sh
export REDIS_DB="redis://vCbdWwjecp@localhost:6379/0"
export REDIS_KEY="generate_ids"
export RABBIT="amqp://user:cAz7aCkLDotl0yRK@localhost:5672"
export ID="queue1"
export KAFKA="localhost:9092"
export KAFKA_TOPIC="queue2"
export TARGET_REST_SERVICE="http://localhost:8080/message"
./service1

