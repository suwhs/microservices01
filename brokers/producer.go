package brokers

import (
	"context"
	"log"
	"time"
)

type ProducerBase struct {
	ProducerBaseInterface
	ctx context.Context
}

func NewProducerBase(ctx context.Context) ProducerBase { return ProducerBase{ctx: ctx} }

func (this *ProducerBase) Context() context.Context {
	return this.ctx
}

func ProduceLoop(this ProducerBaseInterface, fn func(ctx context.Context, msg chan string)) {	
	ch := make(chan string)
	fnCtx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		defer log.Printf("ProduceLoop shutdown")
		for {
			timeout := time.After(time.Second)
			select {
			case msg := <-ch: //  non-blocking reading
				if msg == "" {
					time.Sleep(100 * time.Millisecond)
				} else {
					this.InvokePushMessage(msg)
				}
			case <-this.Context().Done(): // non-blocking reading
				return
			case <-timeout:
			}
		}
	}()
	fn(fnCtx, ch)
	log.Printf("close ch, produce loop finished")
	close(ch)
}
