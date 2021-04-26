package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/mongo"
)


type Msg struct {
	Id   int `json:"id"`
	Value int `json:"value"`
}

func getMongoCollection(mongoURL, dbName, collectionName string) *mongo.Collection {
	clientOptions := options.Client().ApplyURI(mongoURL)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB ... !!")

	db := client.Database(dbName)
	collection := db.Collection(collectionName)
	return collection
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
		CommitInterval: time.Second,
	})
}

func main() {

	// get Mongo db Collection using environment variables.
	mongoURL := os.Getenv("mongoURL")
	dbName := os.Getenv("dbName")
	collectionName := os.Getenv("collectionName")
	collection := getMongoCollection(mongoURL, dbName, collectionName)

	// get kafka reader using environment variables.
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	groupID := "consumer-group-id"
	reader := getKafkaReader(kafkaURL, topic, groupID)


	defer reader.Close()

	fmt.Println("start consuming ... !!")
	msg := Msg{}

	for {
		kafka_msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
		}

		err = json.Unmarshal(kafka_msg.Value, &msg)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(msg)
		fmt.Println(string(kafka_msg.Value))

		insertResult, err := collection.InsertOne(context.Background(), msg)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Inserted a single document: ", insertResult.InsertedID)

	}
}