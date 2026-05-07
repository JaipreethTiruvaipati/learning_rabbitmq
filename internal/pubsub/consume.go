package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Create an "enum" for the acknowledgement type
type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	// 1. Call DeclareAndBind to make sure the queue exists and is bound
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	// 2. Get a new chan of amqp.Delivery structs by using the channel.Consume method
	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer name (empty string means auto-generated)
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	// 3. Start a goroutine that ranges over the channel of deliveries
	go func() {
		for msg := range msgs {
			// 4. Unmarshal the body using the provided unmarshaller
			data, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				msg.Nack(false, false)
				continue
			}

			// Capture the ackType returned by the handler
			ackType := handler(data)

			// Switch on the ackType to determine how to acknowledge the message
			switch ackType {
			case Ack:
				fmt.Println("Acking message")
				msg.Ack(false)
			case NackRequeue:
				fmt.Println("Nacking and requeuing message")
				msg.Nack(false, true)
			case NackDiscard:
				fmt.Println("Nacking and discarding message")
				msg.Nack(false, false)
			}
		}
	}()

	return nil
}

// SubscribeJSON subscribes to a RabbitMQ queue and unmarshals the JSON body into type T.
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			err := json.Unmarshal(data, &target)
			return target, err
		},
	)
}

// SubscribeGob subscribes to a RabbitMQ queue and unmarshals the GOB body into type T.
func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		func(data []byte) (T, error) {
			var target T
			buf := bytes.NewBuffer(data)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&target)
			return target, err
		},
	)
}
