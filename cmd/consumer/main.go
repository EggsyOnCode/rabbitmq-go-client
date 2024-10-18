package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/EggysOnCode/event-driven-rmq/internal"
	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.CreateRmqpConnection("xen", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// another conn for producing callbacks
	publishConn, err := internal.CreateRmqpConnection("xen", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer publishConn.Close()

	// creating a client
	client := internal.NewRabbitMQClient(conn)
	defer client.Close()

	// creating a client
	publishClient := internal.NewRabbitMQClient(conn)
	defer publishClient.Close()

	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	log.Printf("queue 1 is %s", queue.Name)

	// Bind the queues to the excahnge
	if err = client.CreateBinding(queue.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	// start consuming
	msgBus1, err := client.Consume(queue.Name, "consumer-1", false)
	if err != nil {
		panic(err)
	}

	// creating ErrGrps to listen to msg using worker threads // multithreading using errgrp
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// 10 concurrent workers
	g.SetLimit(10)

	go func() {
		for message := range msgBus1 {
			msg := message

			g.Go(func() error {
				fmt.Printf("msg is %s \n", msg.Body)
				time.Sleep(5 * time.Second)
				if err := msg.Ack(false); err != nil {
					log.Printf("failed to ack the msg")
					return err
				}

				// sending to callaback queue
				if err := publishClient.Send(ctx, "customer_callbacks", msg.ReplyTo, amqp091.Publishing{
					Body:          []byte(fmt.Sprintf("Received msg: %s wit coorelation Id %v", string(msg.Body), msg.CorrelationId)),
					CorrelationId: msg.CorrelationId,
					DeliveryMode:  amqp091.Persistent,
					ContentType:   "text/plain",
				}); err != nil {
					log.Printf("failed to send msg to callback queue")
					return err
				}

				log.Printf("Ack msg %s", msg.MessageId)

				return nil
			})

		}
	}()

	select {}
}
