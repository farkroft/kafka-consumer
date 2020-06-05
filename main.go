package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

const (
	topic = "testTopic"
)

func main() {
	kafkaConn := []string{"localhost:9092"}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// create new customer
	master, err := sarama.NewConsumer(kafkaConn, config)
	if err != nil {
		fmt.Println("Error create consumer: ", err.Error())
		os.Exit(1)
	}

	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Println("Error consumer: ", err.Error())
		os.Exit(1)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// count how many messages processed
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})

	// consume without goroutine
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println("Error when consume message: ", err.Error())
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Println("Received message: ", string(msg.Key), string(msg.Value))
			case <-signals:
				fmt.Println("Interupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}
