package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	topic := "order_cafe_topic"
	msgCnt := 0
	broker := []string{"broker:9092"}

	// 1. Create a new consumer
	worker, err := NewConnectConsumer(broker)
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer has been started")

	// 2. Handle OS signals - used to stop the process.
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// 3. Handle messages
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCnt++
				fmt.Printf("Message #%d: %s = %s\n", msgCnt, msg.Topic, string(msg.Value))
				fmt.Printf("Cooking something for %s\n", string(msg.Value))
			case <-sigchan:
				fmt.Println("Terminating consumer")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCnt, "messages")

	// 4. Close the consumer
	if err := worker.Close(); err != nil {
		panic(err)
	}

}

func NewConnectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	var consumer sarama.Consumer
	var err error
	for i := 0; i < 10; i++ {
		consumer, err = sarama.NewConsumer([]string{"broker:9092"}, config)
		if err == nil {
			break
		}
		fmt.Println("Kafka not ready, retrying...", err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		panic("Could not connect to Kafka after retries: " + err.Error())
	}

	return consumer, nil
}
