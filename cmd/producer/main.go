package main

import (
	"fmt"
	k "kafka-golang/internal/kafka"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const (
	topic        = "my-topic"
	numberOfCase = 20
)

func main() {

	var address = []string{"127.0.0.1:9095", "127.0.0.1:9096", "127.0.0.1:9097"}

	p, err := k.NewProducer(address)
	if err != nil {
		logrus.Fatal(err)
	}

	keys := generateUuidString()

	for i := 0; i < 1000; i++ {
		msg := fmt.Sprintf("Kafka message %d", i)
		key := keys[i%numberOfCase]
		err = p.Produce(msg, topic, key)
		if err != nil {
			logrus.Error(err)
		}
	}
}

func generateUuidString() [numberOfCase]string {
	var uuids [numberOfCase]string
	for i := 0; i < numberOfCase; i++ {
		uuids[i] = uuid.NewString()
	}
	return uuids
}
