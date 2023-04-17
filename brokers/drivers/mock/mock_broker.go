package mock

import (
	"context"
	"errors"

	"whs.su/svcs/brokers"
)

func NewMockBroker(bus chan string) brokers.MessageBroker {
	return &MockBroker{bus: bus}
}

type MockBroker struct {
	brokers.BrokerBase
	bus chan string
}

func (this *MockBroker) NewProducerWithContext(ctx context.Context, queue string) brokers.MessagesProducer {
	return &mockProducer{ProducerBase: brokers.NewProducerBase(ctx), bus: this.bus}
}

func (this *MockBroker) NewConsumer(queue string) brokers.MessagesConsumer {
	return &mockConsumer{ConsumerBase: brokers.ConsumerBase{}, bus: this.bus}
}

type mockProducer struct {
	brokers.ProducerBase
	bus chan string
}

func (this *mockProducer) InvokePushMessage(message string) error {
	this.bus <- message
	return nil
}

type mockConsumer struct {
	brokers.ConsumerBase
	bus chan string
}

func (this *mockConsumer) InvokePopMessage(ctx context.Context, ch chan string, onFinish func()) error {
	ch <- <-this.bus
	return nil
}

type factory struct {
}

func (this *factory) New(opts brokers.Options) (brokers.MessageBroker, error) {
	if _channel, ok := opts["bus"]; ok {
		if channel, ok := _channel.(chan string); ok {
			return NewMockBroker(channel), nil
		}
	}
	return nil, errors.New("invalid config for mock broker - missing 'bus' parameter (chan string)")
}

var (
	registry = brokers.RegisterMessageBrokerFactory("mock", &factory{})
)
