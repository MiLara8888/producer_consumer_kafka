package main

import (
	"kafka-golang/internal/handler"
	"kafka-golang/internal/kafka"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

var address = []string{"127.0.0.1:9095", "127.0.0.1:9096", "127.0.0.1:9097"}

const (
	topic         = "my-topic"
	consumerGroup = "my-consumer-group"
)

func main() {

	h := handler.NewHandler()
	c, err := kafka.NewConsumer(h, address, topic, consumerGroup)
	if err != nil {
		logrus.Fatal(err)
	}
	c.Start()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan
	logrus.Fatal(c.Stop())
}
