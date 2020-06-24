// Package main contains the main logic of the "mq-interceptor" microservice.
package main

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"log"
	"os"
	"sync"
)

var db *sql.DB
var mappingMutex sync.Mutex
var cegaPublishChannel *amqp.Channel

func main() {
	var err error

	db, err = sql.Open("postgres", os.Getenv("POSTGRES_CONNECTION"))
	failOnError(err, "Failed to connect to DB")

	legaMQ, err := amqp.DialTLS(os.Getenv("LEGA_MQ_CONNECTION"), getTLSConfig())
	failOnError(err, "Failed to connect to LEGA RabbitMQ")
	legaConsumeChannel, err := legaMQ.Channel()
	failOnError(err, "Failed to create LEGA consume RabbitMQ channel")
	legaPubishChannel, err := legaMQ.Channel()
	failOnError(err, "Failed to create LEGA publish RabbitMQ channel")

	cegaMQ, err := amqp.DialTLS(os.Getenv("CEGA_MQ_CONNECTION"), getTLSConfig())
	failOnError(err, "Failed to connect to CEGA RabbitMQ")
	cegaConsumeChannel, err := cegaMQ.Channel()
	failOnError(err, "Failed to create CEGA consume RabbitMQ channel")
	cegaPublishChannel, err = cegaMQ.Channel()
	failOnError(err, "Failed to create CEGA publish RabbitMQ channel")

	filesDeliveries, err := cegaConsumeChannel.Consume("v1.files", "", false, false, false, false, nil)
	failOnError(err, "Failed to connect to 'v1.files' queue")
	go func() {
		for delivery := range filesDeliveries {
			forwardDeliveryTo(true, cegaConsumeChannel, legaPubishChannel, "", "files", delivery)
		}
	}()

	stableIDsDeliveries, err := cegaConsumeChannel.Consume("v1.stableIDs", "", false, false, false, false, nil)
	failOnError(err, "Failed to connect to 'v1.stableIDs' queue")
	go func() {
		for delivery := range stableIDsDeliveries {
			forwardDeliveryTo(true, cegaConsumeChannel, legaPubishChannel, "", "stableIDs", delivery)
		}
	}()

	errorQueue, err := legaConsumeChannel.QueueDeclare("", false, false, true, false, nil)
	failOnError(err, "Failed to declare 'error' queue")
	err = legaConsumeChannel.QueueBind(errorQueue.Name, "files.error", "cega", false, nil)
	failOnError(err, "Failed to bind 'error' queue")
	errorDeliveries, err := legaConsumeChannel.Consume(errorQueue.Name, "", false, true, false, false, nil)
	failOnError(err, "Failed to connect to 'error' queue")
	go func() {
		for delivery := range errorDeliveries {
			forwardDeliveryTo(false, legaConsumeChannel, cegaPublishChannel, "localega.v1", "files.error", delivery)
		}
	}()

	verifiedQueue, err := legaConsumeChannel.QueueDeclare("", false, false, true, false, nil)
	failOnError(err, "Failed to declare 'verified' queue")
	err = legaConsumeChannel.QueueBind(verifiedQueue.Name, "verified", "lega", false, nil)
	failOnError(err, "Failed to bind 'verified' queue")
	verifiedDeliveries, err := legaConsumeChannel.Consume(verifiedQueue.Name, "", false, true, false, false, nil)
	failOnError(err, "Failed to connect to 'verified' queue")
	go func() {
		for delivery := range verifiedDeliveries {
			forwardDeliveryTo(false, legaConsumeChannel, cegaPublishChannel, "localega.v1", "files.verified", delivery)
		}
	}()

	completedQueue, err := legaConsumeChannel.QueueDeclare("", false, false, true, false, nil)
	failOnError(err, "Failed to declare 'completed' queue")
	err = legaConsumeChannel.QueueBind(completedQueue.Name, "completed", "lega", false, nil)
	failOnError(err, "Failed to bind 'completed' queue")
	completedDeliveries, err := legaConsumeChannel.Consume(completedQueue.Name, "", false, true, false, false, nil)
	failOnError(err, "Failed to connect to 'completed' queue")
	go func() {
		for delivery := range completedDeliveries {
			forwardDeliveryTo(false, legaConsumeChannel, cegaPublishChannel, "localega.v1", "files.completed", delivery)
		}
	}()

	forever := make(chan bool)
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func forwardDeliveryTo(fromCEGAToLEGA bool, channelFrom *amqp.Channel, channelTo *amqp.Channel, exchange string, routingKey string, delivery amqp.Delivery) {
	mappingMutex.Lock()
	defer mappingMutex.Unlock()
	publishing, err := buildPublishingFromDelivery(fromCEGAToLEGA, delivery)
	if err != nil {
		log.Printf("%s", err)
		nackError := channelFrom.Nack(delivery.DeliveryTag, false, false)
		failOnError(nackError, "Failed to Nack message")
		err = publishError(delivery, err)
		failOnError(err, "Failed to publish error message")
	}
	err = channelTo.Publish(exchange, routingKey, false, false, *publishing)
	if err != nil {
		log.Printf("%s", err)
		err := channelFrom.Nack(delivery.DeliveryTag, false, true)
		failOnError(err, "Failed to Nack message")
	} else {
		err = channelFrom.Ack(delivery.DeliveryTag, false)
		failOnError(err, "Failed to Ack message")
		log.Printf("Forwarded message from [%s, %s] to [%s, %s], Correlation ID: %s", delivery.Exchange, delivery.RoutingKey, exchange, routingKey, delivery.CorrelationId)
	}
}

func buildPublishingFromDelivery(fromCEGAToLEGA bool, delivery amqp.Delivery) (*amqp.Publishing, error) {
	publishing := amqp.Publishing{
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
	}

	message := make(map[string]interface{}, 0)
	err := json.Unmarshal(delivery.Body, &message)
	if err != nil {
		return nil, err
	}
	user, ok := message["user"]
	if !ok {
		publishing.Body = delivery.Body
		return &publishing, nil
	}

	stringUser := fmt.Sprintf("%s", user)

	if fromCEGAToLEGA {
		elixirId, err := selectElixirIdByEGAId(stringUser)
		if err != nil {
			return nil, err
		}
		message["user"] = elixirId
	} else {
		egaId, err := selectEgaIdByElixirId(stringUser)
		if err != nil {
			return nil, err
		}
		message["user"] = egaId
	}

	publishing.Body, err = json.Marshal(message)

	return &publishing, err
}

func publishError(delivery amqp.Delivery, err error) error {
	errorMessage := fmt.Sprintf("{\"reason\" : \"%s\", \"original_message\" : \"%s\"}", err.Error(), string(delivery.Body))
	publishing := amqp.Publishing{
		ContentType:     delivery.ContentType,
		ContentEncoding: delivery.ContentEncoding,
		CorrelationId:   delivery.CorrelationId,
		Body:            []byte(errorMessage),
	}
	err = cegaPublishChannel.Publish("localega.v1", "files.error", false, false, publishing)
	return err
}

func selectElixirIdByEGAId(egaId string) (elixirId string, err error) {
	err = db.QueryRow("select elixir_id from mapping where ega_id = $1", egaId).Scan(&elixirId)
	if err == nil {
		log.Printf("Replacing EGA ID [%s] with Elixir ID [%s]", egaId, elixirId)
	}
	return
}

func selectEgaIdByElixirId(elixirId string) (egaId string, err error) {
	err = db.QueryRow("select ega_id from mapping where elixir_id = $1", elixirId).Scan(&egaId)
	if err == nil {
		log.Printf("Replacing Elixir ID [%s] with EGA ID [%s]", elixirId, egaId)
	}
	return
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
