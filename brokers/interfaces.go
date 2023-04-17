package brokers

import "context"

type ProducerBaseInterface interface {
	InvokePushMessage(message string) error
	Context() context.Context	
}

type ConsumerBaseInterface interface {
	InvokePopMessage(ctx context.Context, ch chan string, onFinish func()) error
}


type MessagesConsumer interface {
	ConsumerBaseInterface
}

type MessagesProducer interface {
	ProducerBaseInterface
}

type MessageBroker interface {
	NewProducerWithContext(ctx context.Context, queue string) MessagesProducer
	NewConsumer(queue string) MessagesConsumer
}


type BrokerBaseInterface interface {
	RegisterConsumer(MessagesConsumer) MessagesConsumer
	RegisterProducer(MessagesProducer) MessagesProducer
}

