package service2

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"whs.su/svcs/brokers"
)

type Config struct {
	RedisDB    string `envconfig:"REDIS_DB" required:"true"`
	RedisKey   string `envconfig:"REDIS_KEY" required:"true"`
	RabbitDB   string `envconfig:"RABBIT" required:"true"`
	RabbitKey  string `envconfig:"ID" required:"true"`
	KafkaAddr  string `envconfig:"KAFKA" required:"true"`
	KafkaQueue string `envconfig:"KAFKA_TOPIC" required:"true"`
	HttpPort   string `envconfig:"HTTP_PORT" default:"8080"`
}

type Message struct {
	Id    string `json:"id"`
	Value string `json:"value"`
}

func StartRestService(ctx context.Context, addr string, queue chan Message) {
	router := gin.Default()
	router.POST("/message", func(ctx *gin.Context) {
		var msg Message
		if err := ctx.ShouldBindBodyWith(&msg, binding.JSON); err == nil {
			go func() {
				timeout := time.After(time.Second)
				select {
				case queue <- msg:
				case <-timeout:
					// blocked queue, may be overloaded ?
				}
			}()
		}
	})

	srv := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("could not listen: %s", err.Error())
		}
	}()

	<-ctx.Done()

	srv.Shutdown(ctx)
}

func StartWorkers(workers *sync.WaitGroup, ctx context.Context, datasource1 brokers.MessagesConsumer, resultsProducer brokers.MessagesProducer, queue chan Message) {

	// run broker connections

	workers.Add(1)
	go func() {
		defer workers.Done()
		brokers.ConsumeFunc(datasource1, ctx, func(msg string) bool {
			// log.Printf("redis message: %s", msg)
			return true
		}).Loop(func() {})
		log.Printf("finish read messages from redis")
	}()

	// send data incoming from rest service to rabbitmq
	workers.Add(1)
	go func() {
		defer workers.Done()
		brokers.ProduceLoop(resultsProducer, func(ctx context.Context, ch chan string) {
			for {
				timeout := time.After(1 * time.Second)
				select {
				case <-timeout:
					time.Sleep(100 * time.Millisecond)
					continue
				case msg := <-queue: // service2.Message{ Id: ..., Value: ...}
					if data, err := json.Marshal(msg); err == nil {
					InnerLoop:
						for {
							select {
							case ch <- string(data):
								break InnerLoop
							case <-time.After(100 * time.Millisecond):
								continue InnerLoop
							case <-ctx.Done():
								log.Printf("[1] finish send results to rabbitmq")
								return
							}
						}
					}
				case <-ctx.Done():
					log.Printf("[2] finish send results to rabbitmq")
					return
				}
			}
		})
	}()

	<-ctx.Done()
}
