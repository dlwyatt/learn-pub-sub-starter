package main

import (
	"context"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

func runGame(ctx context.Context) error {
	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	fmt.Printf("connection to RabbitMQ successful\n")

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.Durable,
		handlerLogs(ch),
	)

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
