package rabbitmq

import (
	"errors"
	"log"

	"github.com/streadway/amqp"
	"whs.su/svcs/brokers"
)

type RabbitMQ struct {
	brokers []string
	conn    *amqp.Connection
}

func (this *RabbitMQ) rrhost() string {
	return this.brokers[0] // TODO: failover
}

func (this *RabbitMQ) Connect() error {
	if conn, err := amqp.Dial(this.rrhost()); err != nil {
		return err
	} else {
		this.conn = conn
	}
	return nil
}

func (this *RabbitMQ) Disconnect() error {
	if this.conn != nil {
		defer func() {
			this.conn = nil
		}()
		return this.conn.Close()
	}
	return nil
}

func NewRabbitMQ(brokers []string) brokers.MessageBroker {
	rabbit := &RabbitMQ{brokers: brokers}
	if err := rabbit.Connect(); err != nil {
		log.Fatalf("could not connect to RabbitMQ broker '%s' : %s", rabbit.brokers[0], err.Error())
	}	
	return rabbit
}

type factory struct {}

func (this *factory) New(opts brokers.Options) (brokers.MessageBroker,error) {
	if _host, ok := opts["uri"]; ok {
		if host, ok := _host.(string); ok {
			return NewRabbitMQ([]string{host}), nil
		}
	}
	return nil, errors.New("rabbitmq requires uri parameter")
}

var (
	registry = brokers.RegisterMessageBrokerFactory("rabbitmq",&factory{})
)