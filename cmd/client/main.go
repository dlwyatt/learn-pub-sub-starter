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
	gameOver := runGame(gameContext)

	select {
	case <-gameOver:
		fmt.Printf("Game exiting.\n")
	case <-shutdownSignal:
		fmt.Printf("Received OS shutdown signal.\n")
		cancel()

		select {
		case <-gameOver:
			fmt.Printf("graceful shutdown completed.")
		case <-time.After(5 * time.Second):
			fmt.Printf("game shutdown timeout exceeded.")
		}
	}
}

func runGame(ctx context.Context) chan struct{} {
	done := make(chan struct{})

	go func() {
		fmt.Println("Starting Peril client...")

		conn, err := amqp.Dial(connectionString)
		if err != nil {
			panic(err)
		}

		ch, err := conn.Channel()
		if err != nil {
			panic(err)
		}

		fmt.Printf("connection to RabbitMQ successful\n")

		defer func() { _ = conn.Close() }()

		userName, err := gamelogic.ClientWelcome()
		if err != nil {
			fmt.Printf("Error from client wecome: %v\n", err)
			close(done)
			return
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
			fmt.Printf("Error Subscribing to pause queue: %v\n", err)
			close(done)
			return
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
				close(done)
				return
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
				close(done)
				return
			default:
				fmt.Printf("Comamnd %s is unknown.\n", command)
			}
		}
	}()

	return done
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(state routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(state)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(move)
	}
}
