package kafka

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
	"whs.su/svcs/brokers"
)


type KafkaConsumer struct {
	brokers.ConsumerBase
	reader *kafka.Reader
}

func (this *KafkaConsumer) InvokePopMessage(ctx context.Context, ch chan string, onFinish func()) error {	
	if msg, err := this.reader.ReadMessage(ctx); err == nil {
		ch <- string(msg.Value)
		return nil
	} else {
		return err
	}
}

