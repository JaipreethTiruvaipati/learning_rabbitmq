package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// Create an "enum" for the queue type
type SimpleQueueType int

const (
	SimpleQueueTypeDurable SimpleQueueType = iota
	SimpleQueueTypeTransient
)

// DeclareAndBind creates a channel, declares a queue, and binds it to an exchange
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	// 1. Create a new channel on the connection
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// 2. Set up queue parameters based on the SimpleQueueType
	durable := queueType == SimpleQueueTypeDurable
	autoDelete := queueType == SimpleQueueTypeTransient
	exclusive := queueType == SimpleQueueTypeTransient

	// 3. Declare the queue
	queue, err := ch.QueueDeclare(
		queueName,  // name
		durable,    // durable
		autoDelete, // autoDelete
		exclusive,  // exclusive
		false,      // noWait
		nil,        // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// 4. Bind the queue to the exchange
	err = ch.QueueBind(
		queue.Name, // queue name
		key,        // routing key
		exchange,   // exchange
		false,      // noWait
		nil,        // args
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// 5. Return the channel and the declared queue
	return ch, queue, nil
}
