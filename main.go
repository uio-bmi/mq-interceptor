// Package main contains the main logic of the "mq-interceptor" microservice.
package main

import (
	"crypto/tls"
	_ "database/sql"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"log"
	"os"
	"sync"
)

//var db *sql.DB

var mappingMutex sync.Mutex

func main() {
	var err error

	//db, err = sql.Open("postgres", os.Getenv("DB_CONNECTION"))
	//failOnError(err, "Failed to connect to DB")

	cegaMQ, err := amqp.DialTLS(os.Getenv("CEGA_MQ_CONNECTION"), getTLSConfig())
	failOnError(err, "Failed to connect to CEGA RabbitMQ")
	cegaConsumeChannel, err := cegaMQ.Channel()
	failOnError(err, "Failed to create CEGA consume RabbitMQ channel")
	cegaPublishChannel, err := cegaMQ.Channel()
	failOnError(err, "Failed to create CEGA publish RabbitMQ channel")

	legaMQ, err := amqp.DialTLS(os.Getenv("LEGA_MQ_CONNECTION"), getTLSConfig())
	failOnError(err, "Failed to connect to LEGA RabbitMQ")
	legaConsumeChannel, err := legaMQ.Channel()
	failOnError(err, "Failed to create LEGA consume RabbitMQ channel")
	legaPubishChannel, err := legaMQ.Channel()
	failOnError(err, "Failed to create LEGA publish RabbitMQ channel")

	filesDeliveries, err := cegaConsumeChannel.Consume("v1.files", "", false, false, false, false, nil)
	failOnError(err, "Failed to connect to v1.files queue")
	go func() {
		for delivery := range filesDeliveries {
			forwardDeliveryTo(cegaConsumeChannel, legaPubishChannel, "", "files", delivery)
		}
	}()

	stableIDsDeliveries, err := cegaConsumeChannel.Consume("v1.stableIDs", "", false, false, false, false, nil)
	failOnError(err, "Failed to connect to v1.stableIDs queue")
	go func() {
		for delivery := range stableIDsDeliveries {
			forwardDeliveryTo(cegaConsumeChannel, legaPubishChannel, "", "stableIDs", delivery)
		}
	}()

	errorQueue, err := legaConsumeChannel.QueueDeclare("", false, false, true, false, nil)
	failOnError(err, "Failed to declare error queue")
	err = legaConsumeChannel.QueueBind(errorQueue.Name, "files.error", "cega", false, nil)
	failOnError(err, "Failed to bind error queue")
	errorDeliveries, err := legaConsumeChannel.Consume(errorQueue.Name, "", false, true, false, false, nil)
	failOnError(err, "Failed to connect to error queue")
	go func() {
		for delivery := range errorDeliveries {
			forwardDeliveryTo(legaConsumeChannel, cegaPublishChannel, "localega.v1", "files.error", delivery)
		}
	}()

	processingQueue, err := legaConsumeChannel.QueueDeclare("", false, false, true, false, nil)
	failOnError(err, "Failed to declare processing queue")
	err = legaConsumeChannel.QueueBind(processingQueue.Name, "files.processing", "cega", false, nil)
	failOnError(err, "Failed to bind processing queue")
	processingDeliveries, err := legaConsumeChannel.Consume(processingQueue.Name, "", false, true, false, false, nil)
	failOnError(err, "Failed to connect to processing queue")
	go func() {
		for delivery := range processingDeliveries {
			forwardDeliveryTo(legaConsumeChannel, cegaPublishChannel, "localega.v1", "files.processing", delivery)
		}
	}()

	completedQueue, err := legaConsumeChannel.QueueDeclare("", false, false, true, false, nil)
	failOnError(err, "Failed to declare completed queue")
	err = legaConsumeChannel.QueueBind(completedQueue.Name, "completed", "lega", false, nil)
	failOnError(err, "Failed to bind completed queue")
	completedDeliveries, err := legaConsumeChannel.Consume(completedQueue.Name, "", false, true, false, false, nil)
	failOnError(err, "Failed to connect to completed queue")
	go func() {
		for delivery := range completedDeliveries {
			forwardDeliveryTo(legaConsumeChannel, cegaPublishChannel, "localega.v1", "files.completed", delivery)
		}
	}()

	forever := make(chan bool)
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func forwardDeliveryTo(channelFrom *amqp.Channel, channelTo *amqp.Channel, exchange string, routingKey string, delivery amqp.Delivery) {
	mappingMutex.Lock()
	defer mappingMutex.Unlock()
	err := channelTo.Publish(exchange, routingKey, false, false, buildPublishingFromDelivery(delivery))
	if err != nil {
		err := channelFrom.Nack(delivery.DeliveryTag, false, true)
		failOnError(err, "Failed to Nack message")
	}
	err = channelFrom.Ack(delivery.DeliveryTag, false)
	failOnError(err, "Failed to Ack message")
}

func buildPublishingFromDelivery(delivery amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		Headers:         delivery.Headers,
		ContentType:     delivery.ContentType,
		ContentEncoding: delivery.ContentEncoding,
		DeliveryMode:    delivery.DeliveryMode,
		Priority:        delivery.Priority,
		CorrelationId:   delivery.CorrelationId,
		ReplyTo:         delivery.ReplyTo,
		Expiration:      delivery.Expiration,
		MessageId:       delivery.MessageId,
		Timestamp:       delivery.Timestamp,
		Type:            delivery.Type,
		UserId:          delivery.UserId,
		AppId:           delivery.AppId,
		Body:            delivery.Body,
	}
}

func getTLSConfig() *tls.Config {
	tlsConfig := tls.Config{}
	if os.Getenv("VERIFY_CERT") == "true" {
		tlsConfig.InsecureSkipVerify = false
	} else {
		tlsConfig.InsecureSkipVerify = true
	}
	return &tlsConfig
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
