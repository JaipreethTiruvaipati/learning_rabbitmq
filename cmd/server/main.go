package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerLog() func(routing.GameLog) pubsub.AckType {
	return func(gamelog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Printf("error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}

func main() {
	fmt.Println("Starting Peril server...")

	const connString = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connected to RabbitMQ!")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()
	fmt.Println("Successfully opened a channel!")
	// --- NEW CODE: Subscribe to the game_logs queue ---
	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,    // exchange
		routing.GameLogSlug,           // queueName (game_logs)
		routing.GameLogSlug+".*",      // routing key (game_logs.*)
		pubsub.SimpleQueueTypeDurable, // queueType (durable)
		handlerLog(),
	)
	if err != nil {
		log.Fatalf("could not subscribe to game logs: %v", err)
	}
	fmt.Println("Subscribed to game logs queue successfully!")
	// 1. Print the help menu so the user knows what commands are available
	gamelogic.PrintServerHelp()

	// 2. Start the interactive loop
	for {
		// Wait for the user to type something and hit Enter
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue // They just hit enter, prompt again
		}

		// Look at the first word they typed
		command := words[0]

		switch command {
		case "pause":
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true}, // IsPaused is true
			)
			if err != nil {
				log.Printf("Failed to publish pause message: %v\n", err)
			}

		case "resume":
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false}, // IsPaused is false
			)
			if err != nil {
				log.Printf("Failed to publish resume message: %v\n", err)
			}

		case "quit":
			fmt.Println("Exiting server...")
			return // Exits the loop and shuts down the program

		default:
			fmt.Println("I don't understand that command.")
		}
	}
}
