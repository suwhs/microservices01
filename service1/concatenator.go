package service1

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"whs.su/svcs/brokers"
	"whs.su/svcs/utils"
)

type Config struct {
	RedisDB     string `envconfig:"REDIS_DB" required:"true"`
	RedisKey    string `envconfig:"REDIS_KEY" required:"true"`
	RabbitDB    string `envconfig:"RABBIT" required:"true"`
	RabbitKey   string `envconfig:"ID" required:"true"`
	KafkaAddr   string `envconfig:"KAFKA" required:"true"`
	KafkaQueue  string `envconfig:"KAFKA_TOPIC" required:"true"`
	RestService string `envconfig:"TARGET_REST_SERVICE" required:"true"`
}

func ConcatenatorService(ctx context.Context,
	idsSource brokers.MessagesConsumer,
	valuesSource brokers.MessagesConsumer,
	doConcat func(a, b string)) {

	var workers sync.WaitGroup
	var concatChannel = make(chan string)
	workers.Add(1)
	go brokers.ConsumeFunc(idsSource, ctx, func(id string) bool {
		for {
			select {
			case concatChannel <- id: // send id to second consumer
				return true
			case <-ctx.Done():
				return false
			}
		}
	}).Loop(func() { workers.Done() })

	workers.Add(1)

	valuesBuffer := utils.NewStringFifo()

	go brokers.ConsumeFunc(valuesSource, ctx, func(value string) bool {
		select {
		case id := <-concatChannel: // id
			if valuesBuffer.Size() > 0 {
				// keep current value and use value from fifo
				doConcat(id, valuesBuffer.GetAndPut(value))
			} else {
				doConcat(id, value) // call concatenate function
			}
		case <-ctx.Done():
			log.Printf("message from kafka lost due service terminated (%s)", value)
			return false
		case <-time.After(1 * time.Second):
			log.Printf("timeout while wait for id from ids_source, but value ready: %s", value)
			valuesBuffer.Put(value) // keep current value (no id yet)
		}
		return true
	}).Loop(func() { workers.Done() })

	workers.Wait()
	close(concatChannel)
}

func GeneratorService(keysGenerator brokers.MessagesProducer) {
	brokers.ProduceLoop(keysGenerator, func(ctx context.Context, broker chan string) {
		for {
			guid := uuid.New().String() // create key and publish to broker
			select {
			case <-time.After(time.Second):
			case broker <- guid:
			case <-ctx.Done():
				return
			}
		}
	})
}
