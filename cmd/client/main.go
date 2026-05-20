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

	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		panic(err)
	}

	defer func() { _ = conn.Close() }()
	fmt.Printf("connection to RabbitMQ successful\n")

	go func() {
		userName, err := gamelogic.ClientWelcome()
		if err != nil {
			fmt.Printf("Error from client wecome: %v\n", err)
			close(done)
			return
		}

		ch, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, userName), routing.PauseKey, pubsub.Transient)
		if err != nil {
			fmt.Printf("Error binding client channel: %v\n", err)
			close(done)
			return
		}

		_ = ch
		_ = queue

		inputChan := make(chan []string, 1)

		gameState := gamelogic.NewGameState(userName)

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
				_, err := gameState.CommandMove(inputWords)
				if err != nil {
					fmt.Printf("Error from move command: %v\n", err)
					continue
				}

				fmt.Printf("Move successful.\n")
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
