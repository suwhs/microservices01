package main

import (
	"context"
	"sync"

	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/kelseyhightower/envconfig"
	"whs.su/svcs/brokers"
	_ "whs.su/svcs/brokers/drivers/kafka"
	_ "whs.su/svcs/brokers/drivers/rabbitmq"
	_ "whs.su/svcs/brokers/drivers/redis"
	"whs.su/svcs/service1"
	"whs.su/svcs/service2"
)

/*
service 1:
	connect to redis
	create keys loop -> redis
	connect ro rabbitmq
		loop: read message as id -> X_ID
	connect to kafka
		loop: read from kafka's topic as value -> X_VAL
	send {
		"id" : X_ID,
		"value" : X_VAL
	} -> rest post to service 2
*/

func main() {
	cfg := service1.Config{}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()
	if err := envconfig.Process("", &cfg); err != nil {
		log.Fatalf("configuration error: %s", err.Error())
	}

	redis := brokers.NewBrokerOrFail("redis", map[string]any{"host": cfg.RedisDB})
	rabbit := brokers.NewBrokerOrFail("rabbitmq", map[string]any{"uri": cfg.RabbitDB})
	kafka := brokers.NewBrokerOrFail("kafka", map[string]any{"brokers": cfg.KafkaAddr})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		service1.GeneratorService(redis.NewProducerWithContext(ctx, cfg.RedisKey))
	}()

	http := service1.DefaultHttpInterface{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		service1.ConcatenatorService(ctx,
			rabbit.NewConsumer(cfg.RabbitKey),
			kafka.NewConsumer(cfg.KafkaQueue),
			func(a, b string) {
				//
				j := service2.Message{Id: a, Value: b}
				if data, err := json.Marshal(&j); err == nil {
					if err := http.POST(ctx, cfg.RestService, "application/json", data); err != nil {
						log.Printf("post to %s error: %s", cfg.RestService, err.Error())
					}
				} else {
					log.Printf("error marshal data: %s", err.Error())
				}
			})
	}()

	<-ctx.Done()
	stop()
	wg.Wait()
	log.Printf("service1 shutdown")
}
