package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

func main() {
	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		panic(err)
	}

	defer func() { _ = conn.Close() }()
	fmt.Printf("connection to RabbitMQ successful\n")

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	gamelogic.PrintServerHelp()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	inputChan := make(chan []string, 1)
	for {
		go func() {
			inputChan <- gamelogic.GetInput()
		}()

		select {
		case inputWords := <-inputChan:
			if len(inputWords) == 0 {
				continue
			}

			command := inputWords[0]

			switch {
			case strings.EqualFold(command, "pause"):
				fmt.Println("sending pause message")
				err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
					IsPaused: true,
				})
				if err != nil {
					fmt.Printf("Error sending pause message: %v\n", err)
				}
			case strings.EqualFold(command, "resume"):
				fmt.Println("sending resume message")
				err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
					IsPaused: false,
				})
				if err != nil {
					fmt.Printf("Error sending resume message: %v\n", err)
				}
			case strings.EqualFold(command, "help"):
				gamelogic.PrintServerHelp()
			case strings.EqualFold(command, "quit"):
				gamelogic.PrintQuit()
				return
			default:
				fmt.Printf("Command '%s' is unknown.\n", command)
			}
		case <-sigs:
			fmt.Printf("\nshutting down\n")
			return
		}
	}
}
