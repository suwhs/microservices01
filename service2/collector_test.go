package service2_test

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"testing"
	"time"

	"whs.su/svcs/brokers"
	_ "whs.su/svcs/brokers/drivers/mock"

	"whs.su/svcs/service2"
)

func TestCollectorService(t *testing.T) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	ctx, _ = context.WithTimeout(ctx, 10*time.Second)
	workers := &sync.WaitGroup{}

	queue := make(chan service2.Message)
	datasource1_channel := make(chan string)
	results_channel := make(chan string)

	datasource1 := brokers.NewBrokerOrFail("mock", map[string]any{"bus": datasource1_channel})
	results := brokers.NewBrokerOrFail("mock", map[string]any{"bus": results_channel})

	go func() {

		service2.StartWorkers(workers, ctx, datasource1.NewConsumer("t1"), results.NewProducerWithContext(ctx, <-results_channel), queue)
	}()

	<-ctx.Done()
	stop()
	workers.Wait()
	close(datasource1_channel)
	close(results_channel)
	close(queue)
	log.Printf("test service2 shutdown")
}
