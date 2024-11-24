package kafka

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const (
	consumerGroup = "my-group"
	//блокируемся пока сообщение точно не придёт
	noTimeout = -1
)

type Handler interface {
	HandleMessage(message []byte, offset kafka.TopicPartition, consumerNumber int) error
}

type Consumer struct {
	consumer       *kafka.Consumer
	handler        Handler
	stop           bool
	consumerNumber int
}

func NewConsumer(handler Handler, address []string, topic, consumerGroup string, consumerNumber int) (*Consumer, error) {
	cfg := kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(address, ","),
		"group.id":                 consumerGroup,
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  5000,
		"auto.offset.reset":        "earliest",
	}
	c, err := kafka.NewConsumer(&cfg)
	if err != nil {
		return nil, err
	}

	//подписка на топик
	err = c.Subscribe(topic, nil)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		consumer:       c,
		handler:        handler,
		consumerNumber: consumerNumber}, nil
}

func (c *Consumer) Start() {
	for {
		if c.stop {
			break
		}
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			logrus.Error(err)
		}
		if kafkaMsg == nil {
			continue
		}
		//обработка сообщений
		err = c.handler.HandleMessage(kafkaMsg.Value, kafkaMsg.TopicPartition, c.consumerNumber)
		if err != nil {
			logrus.Error(err)
			continue
		}
		//сохраняет offset смещение на основе предоставленного сообщения локально
		if _, err = c.consumer.StoreMessage(kafkaMsg); err != nil {
			logrus.Error(err)
			continue
		}
	}
}

func (c *Consumer) Stop() error {
	c.stop = true
	_, err := c.consumer.Commit()
	if err != nil {
		logrus.Error(err)
	}
	return c.consumer.Close()
}
