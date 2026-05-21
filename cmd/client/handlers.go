package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(state)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		switch outcome {
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()),
				gamelogic.RecognitionOfWar{Attacker: move.Player, Defender: gs.GetPlayerSnap()},
			)

			if err != nil {
				fmt.Printf("Error publishing war recognition: %v\n", err)
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)

		logEntry := routing.GameLog{
			CurrentTime: time.Now(),
			Username:    rw.Attacker.Username,
		}

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeDraw:
			logEntry.Message = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
		case gamelogic.WarOutcomeYouWon, gamelogic.WarOutcomeOpponentWon:
			logEntry.Message = fmt.Sprintf("%s won a war against %s", winner, loser)
		default:
			fmt.Printf("Unknown war outcome type %v\n", outcome)
			return pubsub.NackDiscard
		}

		err := pubsub.PublishGob(
			ch,
			routing.ExchangePerilTopic,
			fmt.Sprintf("%s.%s", routing.GameLogSlug, rw.Attacker.Username),
			logEntry,
		)

		if err != nil {
			fmt.Printf("Error publishing log entry: %v\n", err)
			return pubsub.NackRequeue
		}

		return pubsub.Ack
	}
}
