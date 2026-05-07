package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

// handlerPause returns a closure that captures the game state and updates it
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

// handlerMove returns a closure that captures the game state and processes army moves
func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {

	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("Error publishing war: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.Ack
		default:
			return pubsub.Ack
		}
	}
}

func publishGameLog(publishCh *amqp.Channel, username string, msg string) error {
	return pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     msg,
			Username:    username,
		},
	)
}

// handlerWar returns a closure that processes war declarations
func handlerWar(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		// HandleWar determines if we are involved, who won, and kills units locally
		outcome, winner, loser := gs.HandleWar(rw)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			// Not our war, throw it back into the queue so the actual participants can grab it!
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			// Error state, just discard it
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(publishCh, rw.Attacker.Username, msg)
			if err != nil {
				fmt.Printf("error publishing game log: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(publishCh, rw.Attacker.Username, msg)
			if err != nil {
				fmt.Printf("error publishing game log: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := publishGameLog(publishCh, rw.Attacker.Username, msg)
			if err != nil {
				fmt.Printf("error publishing game log: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Printf("Error: unknown war outcome: %v\n", outcome)
			return pubsub.NackDiscard
		}
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
		handlerMove(gameState, publishCh),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to army moves: %v", err)
	}
	fmt.Printf("Subscribed to army moves successfully!\n")

	// Subscribe to war declarations
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		"war", // All clients share this single durable queue
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueTypeDurable,
		handlerWar(gameState, publishCh),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe to war messages: %v", err)
	}
	fmt.Printf("Subscribed to war messages successfully!\n")

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
