package service1_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"testing"
	"time"

	"whs.su/svcs/brokers"
	_ "whs.su/svcs/brokers/drivers/mock"
	"whs.su/svcs/service1"
)

func TestConcatenator(t *testing.T) {
	ids_source_channel := make(chan string)
	values_source_channel := make(chan string)
	ids_source := brokers.NewBrokerOrFail("mock", map[string]any{"bus": ids_source_channel})
	values_source := brokers.NewBrokerOrFail("mock", map[string]any{"bus": values_source_channel})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			ids_source_channel <- fmt.Sprintf("id%d", i)
			values_source_channel <- fmt.Sprintf("value-%d", i)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		service1.ConcatenatorService(ctx,
			ids_source.NewConsumer("t1"),
			values_source.NewConsumer("t1"), func(a, b string) {
				log.Printf("id: %s, value: %s", a, b)
			})
	}()
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
	}

	cancel()
	wg.Wait()
}
