package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        bytes,
	})
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(val)

	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buf.Bytes(),
	})
}

type SimpleQueueType uint

const (
	Durable SimpleQueueType = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	var queue amqp.Queue

	ch, err := conn.Channel()
	if err != nil {
		return nil, queue, err
	}

	var durable, autoDelete, exclusive bool
	switch queueType {
	case Durable:
		durable = true
	case Transient:
		autoDelete = true
		exclusive = true
	default:
		return nil, queue, fmt.Errorf("Value %v is not a known SimpleQueueType.", queueType)
	}

	queue, err = ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, amqp.Table{
		"x-dead-letter-exchange": routing.ExchangePerilDeadLetter,
	})
	if err != nil {
		return nil, queue, err
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, queue, err
	}

	return ch, queue, nil
}

type AckType uint

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveries {
			var body T
			err := json.Unmarshal(delivery.Body, &body)
			if err != nil {
				fmt.Printf("Error unmarshalling message: %v\n", err)
				continue
			}

			ackType := handler(body)

			switch ackType {
			case Ack:
				fmt.Printf("Acking message.\n")
				err = delivery.Ack(false)
				if err != nil {
					fmt.Printf("error acknolwedging delivery: %v\n", err)
				}
			case NackDiscard:
				fmt.Printf("Nacking message with discard.\n")
				err = delivery.Nack(false, false)
				if err != nil {
					fmt.Printf("error acknolwedging delivery: %v\n", err)
				}
			case NackRequeue:
				fmt.Printf("Nacking message with requeue.\n")
				err = delivery.Nack(false, true)
				if err != nil {
					fmt.Printf("error acknolwedging delivery: %v\n", err)
				}
			default:
				fmt.Printf("Unknown AckType %v\n", ackType)
			}

		}
	}()

	return nil
}
