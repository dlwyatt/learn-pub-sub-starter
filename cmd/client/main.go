package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

func main() {
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	gameContext, cancel := context.WithCancel(context.Background())
	gameError := make(chan error, 1)
	go func() {
		gameError <- runGame(gameContext)
	}()

	select {
	case err := <-gameError:
		if err != nil {
			fmt.Printf("game error: %v\n", err)
		} else {
			fmt.Printf("Game exiting.\n")
		}
	case <-shutdownSignal:
		fmt.Printf("Received OS shutdown signal.\n")
		cancel()

		select {
		case err := <-gameError:
			if err != nil {
				fmt.Printf("error during shutdown: %v\n", err)
			} else {
				fmt.Printf("graceful shutdown completed.\n")
			}
		case <-time.After(5 * time.Second):
			fmt.Printf("game shutdown timeout exceeded.\n")
		}
	}
}

func runGame(ctx context.Context) error {
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	fmt.Printf("connection to RabbitMQ successful\n")

	defer func() { _ = conn.Close() }()

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		return err
	}

	gameState := gamelogic.NewGameState(userName)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, userName),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	)

	if err != nil {
		return err
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.Transient,
		handlerMove(gameState),
	)

	inputChan := make(chan []string, 1)

	for {
		go func() {
			inputChan <- gamelogic.GetInput()
		}()

		var inputWords []string

		select {
		case inputWords = <-inputChan:
		case <-ctx.Done():
			return nil
		}

		if len(inputWords) == 0 {
			continue
		}

		command := inputWords[0]

		switch {
		case strings.EqualFold(command, "spawn"):
			err := gameState.CommandSpawn(inputWords)
			if err != nil {
				fmt.Printf("Error from spawn command: %v\n", err)
			}
		case strings.EqualFold(command, "move"):
			move, err := gameState.CommandMove(inputWords)
			if err != nil {
				fmt.Printf("Error from move command: %v\n", err)
				continue
			}

			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName), move)
			if err != nil {
				fmt.Printf("error sending move to RabbitMQ: %v\n", err)
				continue
			}

			fmt.Printf("move published successfully.\n")
		case strings.EqualFold(command, "status"):
			gameState.CommandStatus()
		case strings.EqualFold(command, "help"):
			gamelogic.PrintClientHelp()
		case strings.EqualFold(command, "spam"):
			fmt.Printf("Spamming not allowed yet.\n")
		case strings.EqualFold(command, "quit"):
			gamelogic.PrintQuit()
			return nil
		default:
			fmt.Printf("Comamnd %s is unknown.\n", command)
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(state)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		if outcome == gamelogic.MoveOutComeSafe || outcome == gamelogic.MoveOutcomeMakeWar {
			return pubsub.Ack
		}

		return pubsub.NackDiscard
	}
}
