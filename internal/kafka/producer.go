package kafka

import (
	"errors"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	producer *kafka.Producer
}

const(
	flushTimeout = 5000 //ms
)

func NewProducer(address []string) (*Producer, error) {

	//настройки https://github.com/confluentinc/librdkafka/tree/master/CONFIGURATION.md
	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(address, ","),
	}
	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, err
	}
	return &Producer{producer: p}, nil
}

// отправка сообщений
func (p *Producer) Produce(message, topic, key string) error {

	kafkaMsg := &kafka.Message{
		//в какую партицию
		TopicPartition: kafka.TopicPartition{Topic: &topic,
			//самораспределение
			Partition: kafka.PartitionAny},
		Value: []byte(message),
		//ключ для распределения по партициям
		Key:   []byte(key),
	}
	//обратная связь о статусе
	kafkaChan := make(chan kafka.Event)
	//отправка
	err := p.producer.Produce(kafkaMsg, kafkaChan)
	if err != nil {
		return err

	}

	//инфо о доставке сообщения
	e := <-kafkaChan

	switch ev := e.(type) {
	case *kafka.Message:
		return nil
	case kafka.Error:
		return ev
	default:
		return errors.New("UnknowType")
	}
}

func (p *Producer) Close() {

	//завершение отправки в течении 5 секкунд
	p.producer.Flush(flushTimeout)
	p.producer.Close()
}
