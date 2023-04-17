package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"whs.su/svcs/brokers"
	_ "whs.su/svcs/brokers/drivers/kafka"
	_ "whs.su/svcs/brokers/drivers/rabbitmq"
	_ "whs.su/svcs/brokers/drivers/redis"
)

func BrokerTest(parent context.Context,
	producer brokers.MessagesProducer,
	consumer brokers.MessagesConsumer) {
	ctx, cancel := context.WithCancel(parent)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go brokers.ProduceLoop(producer, func(ctx context.Context, ch chan string) {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			ch <- fmt.Sprintf("counter %d", i)
		}
		log.Printf("finish produce 100 samples")
	})

	wg.Add(1)
	go brokers.ConsumeFunc(consumer, ctx, func(msg string) bool {
		if msg == "" {
			return true
		}
		//  log.Printf("receive '%s'", msg)
		return true
	}).Loop(func() { wg.Done() })

	<-time.After(1 * time.Second)
	cancel()

	wg.Wait()
	log.Printf("done")

}

func main() {
	log.Printf("test rabbitmq driver")
	if rabbit, err := brokers.NewBroker("rabbitmq", map[string]any{
		"uri": "amqp://user:cAz7aCkLDotl0yRK@localhost:5672",
	}); err == nil {
		ctx := context.TODO()
		producer := rabbit.NewProducerWithContext(ctx, "queue1")
		consumer := rabbit.NewConsumer("queue1")

		BrokerTest(ctx, producer, consumer)

	}
	log.Printf("test kafka driver")
	if kafka, err := brokers.NewBroker("kafka", map[string]any{"brokers": "localhost:9092"}); err == nil {
		ctx := context.TODO()
		producer := kafka.NewProducerWithContext(ctx, "queue2")
		consumer := kafka.NewConsumer("queue2")
		BrokerTest(ctx, producer, consumer)
	}
	log.Printf("test redis driver")
	if redis, err := brokers.NewBroker("redis", map[string]any{
		"host":     "localhost:6379",
		"password": "vCbdWwjecp",
	}); err == nil {
		ctx := context.TODO()
		producer := redis.NewProducerWithContext(ctx, "queue-redis")
		consumer := redis.NewConsumer("queue-redis")
		BrokerTest(ctx, producer, consumer)
	}
}
