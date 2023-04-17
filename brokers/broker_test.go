package brokers_test

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"whs.su/svcs/brokers"
	_ "whs.su/svcs/brokers/drivers/mock"
)

func TestSyntetic_01(t *testing.T) {
	bus := make(chan string)

	ba := brokers.NewBrokerOrFail("mock", map[string]any{"bus": bus})
	bb := brokers.NewBrokerOrFail("mock", map[string]any{"bus": bus})

	baseCtx := context.Background()
	ctx, cancel := context.WithCancel(baseCtx)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		// NB: ProduceLoop accept loop functon as argument
		brokers.ProduceLoop(ba.NewProducerWithContext(ctx, "Q"), func(ctx context.Context, ch chan string) {
			for {
				t.Logf("produce message...")
				ch <- "hello"
				timeout := time.After(300 * time.Millisecond)
				select {
				case <-ctx.Done():
					t.Logf("mock produce loop exit")
					return
				case <-timeout:
				}

			}

		})
	}()

	wg.Add(1)

	go func() {
		brokers.ConsumeFunc(bb.NewConsumer("Q"), ctx, func(msg string) bool {
			t.Logf("consume message: '%s'", msg)
			return true
		}).Loop(func() {
			t.Logf("mock consume loop exit")
			wg.Done()
		})
	}()

	{
		buf := make([]byte, 0)
		n := runtime.Stack(buf, true)
		t.Logf("runtime stack: %d\n%s", n, string(buf))
	}

	timeout := time.After(4 * time.Second)
	unclean_timeout := time.After(5 * time.Second)
	select {
	case <-timeout:
		cancel()
	case <-unclean_timeout:
		cancel()
		t.Fatalf("brokers loop shutown failed, no wait")
		return
	}
	wg.Wait()
	close(bus)
	t.Logf("graceful shutdown")

	t.Logf("goroutines:%d", runtime.NumGoroutine())
}
