package main

/*
 service 2:
	listen rest :
		on msg from service 2:
			send msg to rabbitmq
	--
	connect to rabbit
	// connect to kafka

*/
import (
	"context"
	"fmt"
	"sync"

	// "fmt"
	"log"
	"os"
	"os/signal"

	"github.com/kelseyhightower/envconfig"
	"whs.su/svcs/brokers"
	_ "whs.su/svcs/brokers/drivers/kafka"
	_ "whs.su/svcs/brokers/drivers/rabbitmq"
	_ "whs.su/svcs/brokers/drivers/redis"
	"whs.su/svcs/service2"
)

func main() {

	cfg := service2.Config{}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	if err := envconfig.Process("", &cfg); err != nil {
		log.Fatalf("configuration error: %s", err.Error())
	}
	var queue = make(chan service2.Message)
	workers := &sync.WaitGroup{}

	workers.Add(1)
	go func() {
		defer workers.Done()
		service2.StartRestService(ctx, fmt.Sprintf("localhost:%s", cfg.HttpPort), queue)
		log.Printf("stop web server")
	}()

	redis := brokers.NewBrokerOrFail("redis", map[string]any{"host": cfg.RedisDB})
	rabbit := brokers.NewBrokerOrFail("rabbitmq", map[string]any{"uri": cfg.RabbitDB})
	service2.StartWorkers(workers, ctx, redis.NewConsumer(cfg.RedisKey), rabbit.NewProducerWithContext(ctx, cfg.RabbitKey), queue)

	stop()
	workers.Wait()
	close(queue)
	log.Printf("service2 shutdown")
}
