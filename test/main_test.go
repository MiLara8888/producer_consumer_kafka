package test

import (
	"fmt"
	k "kafka-golang/internal/kafka"
	"testing"

	"github.com/sirupsen/logrus"
)

const (
	topic = "my-topic"
)

func TestProduce(t *testing.T) {
	var address = []string{"127.0.0.1:9095", "127.0.0.1:9096", "127.0.0.1:9097"}

	p, err := k.NewProducer(address)
	if err != nil {
		logrus.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("Kafka message %d", i)
		err = p.Produce(msg, topic)
		if err != nil {
			logrus.Error(err)
		}
	}
}
