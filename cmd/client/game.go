package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

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
		handlerMove(gameState, ch),
	)

	if err != nil {
		return err
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.Durable,
		handlerWar(gameState),
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
