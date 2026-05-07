package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SubscribeJSON subscribes to a RabbitMQ queue and unmarshals the JSON body into type T.
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	// 1. Call DeclareAndBind to make sure the queue exists and is bound
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
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
			var data T
			
			// 4. Unmarshal the body (raw bytes) of each message into the (generic) T type
			err := json.Unmarshal(msg.Body, &data)
			if err != nil {
				// If there's an error unmarshaling, ack the message anyway so it doesn't get stuck
				msg.Ack(false)
				continue
			}

			// 5. Call the given handler function with the unmarshaled message
			handler(data)

			// 6. Acknowledge the message with delivery.Ack(false) to remove it from the queue
			msg.Ack(false)
		}
	}()

	return nil
}
