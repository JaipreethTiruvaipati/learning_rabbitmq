package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishJSON takes any type T, marshals it to JSON, and publishes it to the specified exchange.
func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	// 1. Marshal the val to JSON bytes
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	// 2. Publish the message using the channel
	err = ch.PublishWithContext(
		context.Background(), // ctx
		exchange,             // exchange name
		key,                  // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return err
	}

	return nil
}
