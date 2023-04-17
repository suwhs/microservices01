package brokers

import (
	"context"
	"errors"
	"log"
	"sync"
)

type Options map[string]any

type MessageBrokerFactory interface {
	New(Options) (MessageBroker, error)
}

var (
	ErrorBrokerFactoryAlreadyExists = errors.New("broker factory already registered")
	ErrorUnknownBrokerFactory       = errors.New("broker factory not found")
)

type BrokerBase struct {
	consumers []MessagesConsumer
	producers []MessagesProducer
	lock      sync.Mutex
	ctx       context.Context
}

func (this *BrokerBase) RegisterConsumer(c MessagesConsumer) MessagesConsumer {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.consumers = append(this.consumers, c)
	return c
}

func (this *BrokerBase) RegisterProducer(p MessagesProducer) MessagesProducer {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.producers = append(this.producers, p)
	return p
}

var (
	registryLock sync.Mutex
	registry     map[string]MessageBrokerFactory = make(map[string]MessageBrokerFactory)
)

func RegisterMessageBrokerFactory(name string, brokerFactory MessageBrokerFactory) error {
	registryLock.Lock()
	defer registryLock.Unlock()
	if _, ok := registry[name]; ok {
		return ErrorBrokerFactoryAlreadyExists
	}
	registry[name] = brokerFactory
	log.Printf("register broker driver: %s", name)
	return nil
}

func NewBroker(name string, opts Options) (MessageBroker, error) {
	registryLock.Lock()
	defer registryLock.Unlock()
	if factory, ok := registry[name]; ok {
		return factory.New(opts)
	}
	log.Printf("not found '%s' in %v", name, registry)
	return nil, ErrorUnknownBrokerFactory
}
