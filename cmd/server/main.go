package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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
	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()
	fmt.Printf("connection to RabbitMQ successful\n")

	ch, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.Durable)
	if err != nil {
		return err
	}

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
		case strings.EqualFold(command, "pause"):
			fmt.Println("sending pause message")
			err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				fmt.Printf("Error sending pause message: %v\n", err)
			}
		case strings.EqualFold(command, "resume"):
			fmt.Println("sending resume message")
			err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				fmt.Printf("Error sending resume message: %v\n", err)
			}
		case strings.EqualFold(command, "help"):
			gamelogic.PrintServerHelp()
		case strings.EqualFold(command, "quit"):
			gamelogic.PrintQuit()
			return nil
		default:
			fmt.Printf("Command '%s' is unknown.\n", command)
		}
	}
}
