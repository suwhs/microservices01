package kafka

import (
	"context"
	"errors"

	kafka "github.com/segmentio/kafka-go"
	"whs.su/svcs/brokers"
)

type Kafka struct {
	brokers.BrokerBase
	brokers []string
}

// TODO: options
func NewKafka(brokers []string) brokers.MessageBroker {
	k := &Kafka{brokers: brokers}

	return k
}

func (this *Kafka) NewConsumer(queue string) brokers.MessagesConsumer {
	return &KafkaConsumer{reader: kafka.NewReader(kafka.ReaderConfig{
		Brokers: this.brokers,
		Topic:   queue,
		GroupID: "msvc",
	})}
}

func (this *Kafka) NewProducerWithContext(ctx context.Context, queue string) brokers.MessagesProducer {
	return &KafkaProducer{ProducerBase: brokers.NewProducerBase(ctx),
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: this.brokers,
			Topic:   queue,
		})}
}

type factory struct {
}

func (this *factory) New(opts brokers.Options) (brokers.MessageBroker, error) {
	if _host, ok := opts["brokers"]; ok {
		if host, ok := _host.(string); ok {
			return NewKafka([]string{host}),nil
		}
	}	
	return nil, errors.New("kafka requires 'brokers' param")
}

var (
	registry = brokers.RegisterMessageBrokerFactory("kafka", &factory{})
)
