package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

const connectionString = "amqp://guest:guest@localhost:5672/"

func main() {
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		panic(err)
	}

	defer func() { _ = conn.Close() }()

	fmt.Printf("connection successful\n")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-sigs

	fmt.Printf("\nshutting down\n")
}
