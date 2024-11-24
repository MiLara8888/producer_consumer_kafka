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
	c1, err := kafka.NewConsumer(h, address, topic, consumerGroup, 1)
	if err != nil {
		logrus.Fatal(err)
	}
	c2, err := kafka.NewConsumer(h, address, topic, consumerGroup, 2)
	if err != nil {
		logrus.Fatal(err)
	}
	c3, err := kafka.NewConsumer(h, address, topic, consumerGroup, 3)
	if err != nil {
		logrus.Fatal(err)
	}
	go func ()  {
		c1.Start()
	}()
	go func ()  {
		c2.Start()
	}()
	go func ()  {
		c3.Start()
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	<-signalChan
	logrus.Fatal(c2.Stop(), c1.Stop(), c3.Stop())
}
