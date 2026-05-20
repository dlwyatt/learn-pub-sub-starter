package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

func main() {
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		panic(err)
	}

	defer func() { _ = conn.Close() }()
	fmt.Printf("connection successful\n")

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})

	if err != nil {
		panic(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	fmt.Printf("\nshutting down\n")
}
