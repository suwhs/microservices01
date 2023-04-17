package redis

import (
	"context"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	redis "github.com/go-redis/redis/v9"
	"whs.su/svcs/brokers"
)

type Redis struct {
	brokers.BrokerBase
	client *redis.Client
}

func NewRedisBroker(host string, user string, password string, dbi int) brokers.MessageBroker {
	result := &Redis{}
	log.Printf("new redis client %s with password %s [db:%d]", host, password, dbi)
	result.client = redis.NewClient(&redis.Options{
		Addr:     host,
		Password: password,
		DB:       dbi,
	})

	return result
}

func (this *Redis) NewProducerWithContext(ctx context.Context, queue string) brokers.MessagesProducer {
	c, cancel := context.WithCancel(ctx)

	return this.RegisterProducer(&RedisProducerWithContext{
		ProducerBase: brokers.NewProducerBase(c),
		cancel:       cancel,
		client:       this.client,
		queue:        queue,
	})
}

func (this *Redis) NewConsumer(queue string) brokers.MessagesConsumer {
	return this.RegisterConsumer(&RedisConsumer{
		client: this.client,
		queue:  queue,
	})
}

type RedisProducerWithContext struct {
	brokers.ProducerBase
	client *redis.Client
	cancel context.CancelFunc
	queue  string
}

func (this *RedisProducerWithContext) InvokePushMessage(msg string) error {
	return this.client.RPush(this.Context(), this.queue, msg).Err()
}

type RedisConsumer struct {
	brokers.ConsumerBase
	client *redis.Client
	queue  string
}

func (this *RedisConsumer) InvokePopMessage(ctx context.Context, ch chan string, onFinish func()) error {

	sendToChannel := func(m string) error {
		select {
		case ch <- m:
		case <-ctx.Done():
			onFinish()
		}
		return nil
	}
	if results, err := this.client.BLPop(ctx, 1*time.Second, this.queue).Result(); err == nil || errors.Unwrap(err) == nil {
		if len(results) > 0 {
			return sendToChannel(results[1])
		}
		return nil
	} else {
		if err, ok := err.(net.Error); ok && err.Timeout() {
			// redis timeout - no data yet
			time.Sleep(100 * time.Millisecond)
			log.Printf("redis timeout")
			return nil
		} else {
			log.Printf("unknown redis error: %v", err)
			// other fatal error ?
			// retry connection/failover ?
		}
	}
	onFinish()
	return net.ErrClosed
}

type factory struct {
}

func mustString(opts brokers.Options, key string) (string, bool) {
	if _val, ok := opts[key]; ok {
		if str, ok := _val.(string); ok {
			return str, true
		}
	}
	return "", false
}

func mustInt(opts brokers.Options, key string, defaultValue int) (int, bool) {
	if _val, ok := opts[key]; ok {
		if str, ok := _val.(string); ok {
			if num, err := strconv.Atoi(str); err == nil {
				return num, true
			}

		} else if num, ok := _val.(int); ok {
			return num, true
		}
	}
	return defaultValue, false
}

func SplitUri(uri string) (host string, user string, password string, db string, err error) {
	db = "0"
	host = uri

	if strings.HasPrefix(uri, "redis://") {
		host = strings.Replace(host, "redis://", "", 1)
	}

	if strings.Contains(host, "/") {
		parts := strings.Split(host, "/")
		if len(parts) == 2 {
			host = parts[0]
			db = parts[1]
		}
	}
	if strings.Contains(host, "@") {
		parts := strings.Split(host, "@")
		if len(parts) == 2 {
			host = parts[1]
			password = parts[0]
		}
	}
	return host, user, password, db, nil
}

func (this *factory) New(opts brokers.Options) (brokers.MessageBroker, error) {
	if host, ok := mustString(opts, "host"); ok {
		log.Printf("connect redis uri: %s", host)

		var err error
		defaultDb := 0
		password := ""
		user := ""
		dbs := "0"

		host, user, password, dbs, err = SplitUri(host)

		if password == "" {
			password, _ = mustString(opts, "password")
		}

		defaultDb, _ = strconv.Atoi(dbs)
		db, _ := mustInt(opts, "db", defaultDb)
		return NewRedisBroker(host, user, password, db), err
	}
	return nil, errors.New("redis broker instance opts must contains \"host\" parameter")
}

var (
	registered = brokers.RegisterMessageBrokerFactory("redis", &factory{})
)
