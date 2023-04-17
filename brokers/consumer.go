package brokers

import (
	"context"
	"log"
)

type ConsumerBase struct {
	ConsumerBaseInterface
	queue string
}

type ConsumeDelegate struct {
	ctx    context.Context
	cancel context.CancelFunc
	ch     chan string
	method func(msg string) bool
}

func (this *ConsumeDelegate) CancelContext() {
	this.cancel()
}

func (this *ConsumeDelegate) Loop(onFinish func()) {
	for {
		select {
		case msg := <-this.ch:
			this.method(msg)
		case <-this.ctx.Done():
			onFinish()
			return
		}
	}
}

func NewConsumeDelegate(parent context.Context, msg chan string, method func(msg string) bool) *ConsumeDelegate {
	c, cancel := context.WithCancel(parent)
	return &ConsumeDelegate{
		ctx:    c,
		cancel: cancel,
		method: method,
		ch:     msg,
	}
}

func ConsumeFunc(this ConsumerBaseInterface, ctx context.Context, fn func(msg string) bool) *ConsumeDelegate {
	ch := make(chan string)
	delegate := NewConsumeDelegate(ctx, ch, fn)
	go func() {
		defer log.Printf("ConsumeFunc inner loop shutdown gracefully")
		for {
			if err := this.InvokePopMessage(ctx, ch, func() {
				close(ch)
			}); err != nil {
				log.Printf("fetch message error %s", err.Error())
				return
			}
		}
	}()
	return delegate
}
