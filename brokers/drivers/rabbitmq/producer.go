package rabbitmq

import (
	"context"
	"log"

	"github.com/streadway/amqp"
	"whs.su/svcs/brokers"
)


type RabbitProducer struct {
	brokers.ProducerBase
	queue            string
	channel          *amqp.Channel
	mqq              amqp.Queue
	autoincrementKey int
}

func (this *RabbitMQ) NewProducerWithContext(ctx context.Context, queue string) brokers.MessagesProducer {
	if channel, err := this.conn.Channel(); err != nil {
		log.Fatalf("could not create rabbitmq channel : %s", err.Error())
	} else {
		if mqq, err := channel.QueueDeclare(queue, false, false, false, false, nil); err != nil {
			log.Fatalf("could not declare rabbitmq queue : %s", err.Error())
		} else {
			log.Printf("rabbitmq queue declared: '%s'", queue)
			return &RabbitProducer{ProducerBase: brokers.NewProducerBase(ctx), channel: channel, mqq: mqq, queue: queue}
		}
	}
	return nil
}

func (this *RabbitProducer) InvokePushMessage(message string) error {
	if err := this.channel.Publish("", this.queue, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message),
	}); err != nil {
		log.Fatalf("could not publish message to channel")		
	}
	return nil
}