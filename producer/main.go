package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type Order struct {
	CustomerName string `json:"customer_name"`
	Type         string `json:"type"`
	Amount       int    `json:"amount"`
	Menu         string `json:"menu"`
}

func main() {
	http.HandleFunc("/order", PlaceOrder)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func PlaceOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 1. Parse request body into order
	var order Order
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	orderData, err := json.Marshal(order)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 2. Push order to Kafka
	topic := "order_cafe_topic"
	err = PushOrderToQueue(topic, orderData)

	response := map[string]string{
		"message": "Order for " + order.CustomerName + " placed successfully!",
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Println(err)
		http.Error(w, "Error placing order", http.StatusInternalServerError)
		return
	}
}

func PushOrderToQueue(topic string, message []byte) error {
	broker := []string{"broker:9092"}
	// Create connection
	producer, err := NewConnectProducer(broker)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	// Send message
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("Order is stored in topic(%s)/partition(%d)/offset(%d)\n",
		topic,
		partition,
		offset)

	return nil
}

func NewConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	return sarama.NewSyncProducer(brokers, config)
}
