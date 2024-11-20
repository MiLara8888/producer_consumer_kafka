package main

import (
	"fmt"
	k "kafka-golang/internal/kafka"

	"github.com/sirupsen/logrus"
)

const (
	topic = "my-topic"
)

func main() {

	var address = []string{"localhost:9091", "localhost:9092", "localhost:9093"}

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
