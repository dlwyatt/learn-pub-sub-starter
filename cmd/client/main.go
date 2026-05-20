package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

func main() {
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		panic(err)
	}

	defer func() { _ = conn.Close() }()
	fmt.Printf("connection to RabbitMQ successful\n")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		panic(err)
	}

	ch, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, userName), routing.PauseKey, pubsub.Transient)
	if err != nil {
		panic(err)
	}

	// just making compiler happy at this interim step
	_ = ch
	_ = queue

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	fmt.Printf("\nshutting down\n")
}
