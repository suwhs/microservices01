package rabbitmq

import (
	"context"
	"log"
	"time"

	"github.com/streadway/amqp"
	"whs.su/svcs/brokers"
)


type RabbitConsumer struct {
	brokers.ConsumerBase
	conn    *amqp.Connection
	channel *amqp.Channel
	mqq     <-chan amqp.Delivery
}

func (this *RabbitMQ) NewConsumer(queue string) brokers.MessagesConsumer {
	if channel, err := this.conn.Channel(); err != nil {
		log.Fatalf("could not create rabbitmq channel: %s ", err.Error())
	} else {
		if mqq, err := channel.Consume(queue, "", true, false, false, false, nil); err != nil {
			log.Fatalf("could not consume rabbitmq queue : %s", err.Error())
		} else {
			return &RabbitConsumer{
				channel: channel,
				conn:    this.conn,
				mqq:     mqq,
			}
		}
	}
	return nil
}


func (this *RabbitConsumer) InvokePopMessage(ctx context.Context, ch chan string, onFinish func()) error {	
	timeout := time.After(1000 * time.Millisecond)
	select {
	case msg := <-this.mqq:
		ch <- string(msg.Body)
	case <-timeout:
	}
	return nil
}

