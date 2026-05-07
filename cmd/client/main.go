package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

// handlerPause returns a closure that captures the game state and updates it
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		// Use defer to display a new prompt when the function exits
		defer fmt.Print("> ")

		// Use the game state's HandlePause method to pause the game for the client
		gs.HandlePause(ps)
	}
}

// handlerMove returns a closure that captures the game state and processes army moves
func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(move)
	}
}

func main() {
	fmt.Println("Starting Peril client...")
	// 1. Connect to RabbitMQ
	const connString = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connected to RabbitMQ!")
	// 2. Prompt the user for their username using the gamelogic package
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to get username: %v", err)
	}
	// 3. Construct the queue name using the routing key and the username
	queueName := routing.PauseKey + "." + username
	// 4. Create a new GameState for the player FIRST
	// (We need the gameState to pass into the handler)
	gameState := gamelogic.NewGameState(username)

	// 5. Replace previous DeclareAndBind with pubsub.SubscribeJSON
	err = pubsub.SubscribeJSON(
		conn,                            // connection
		routing.ExchangePerilDirect,     // direct exchange
		queueName,                       // pause.username
		routing.PauseKey,                // routing key "pause"
		pubsub.SimpleQueueTypeTransient, // transient queue type
		handlerPause(gameState),         // the new handler we just created
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to pause messages: %v", err)
	}
	fmt.Printf("Subscribed to queue %s successfully!\n", queueName)

	// Open a channel specifically for publishing messages later
	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	// Subscribe to army moves from ALL players (army_moves.*)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gameState.GetUsername(), // queue name: army_moves.username
		routing.ArmyMovesPrefix+".*",                        // routing key: army_moves.*
		pubsub.SimpleQueueTypeTransient,
		handlerMove(gameState),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to army moves: %v", err)
	}
	fmt.Printf("Subscribed to army moves successfully!\n")

	// 6. Start the interactive REPL loop
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		command := words[0]

		switch command {
		case "spawn":
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Error spawning unit: %v\n", err)
			}

		case "move":
			// We changed _ to move so we can access the returned ArmyMove data
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
				continue
			}

			// Broadcast the move to all other clients using the peril_topic exchange
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+move.Player.Username, // routing key: army_moves.username
				move,
			)
			if err != nil {
				fmt.Printf("Error publishing move: %v\n", err)
				continue
			}
			
			// Log a message to the console stating that the move was published successfully
			fmt.Println("Move published successfully!")

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return // Exits the loop and shuts down the program

		default:
			fmt.Println("I don't understand that command.")
		}
	}

}
