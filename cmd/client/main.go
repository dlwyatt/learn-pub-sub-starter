package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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
