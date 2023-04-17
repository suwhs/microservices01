package kafka

import (
	"encoding/binary"

	kafka "github.com/segmentio/kafka-go"
	"whs.su/svcs/brokers"
)

type KafkaProducer struct {
	brokers.ProducerBase
	writer           *kafka.Writer
	autoincrementKey uint32
	keyBuf           [4]byte
}

func (this *KafkaProducer) InvokePushMessage(message string) error {
	var keyBuf = make([]byte, 4)	
	binary.LittleEndian.PutUint32(keyBuf, this.autoincrementKey)
	this.autoincrementKey++
	if err := this.writer.WriteMessages(this.Context(), kafka.Message{
		Key:   keyBuf,
		Value: []byte(message),
	}); err != nil {
		return err
	}
	return nil
}

