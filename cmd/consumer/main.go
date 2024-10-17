package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/EggysOnCode/event-driven-rmq/internal"
	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.CreateRmqpConnection("xen", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// creating a client
	client := internal.NewRabbitMQClient(conn)
	defer client.Close()

	client2 := internal.NewRabbitMQClient(conn)
	defer client2.Close()

	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	log.Printf("queue 1 is %s", queue.Name)

	queue2, err := client2.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}


	log.Printf("queue 2 is %s", queue2.Name)

	// Bind the queues to the excahnge
	if err = client.CreateBinding(queue.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	if err = client2.CreateBinding(queue2.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	// start consuming
	msgBus1, err := client.Consume(queue.Name, "consumer-1", false)
	if err != nil {
		panic(err)
	}

	msgBus2, err := client2.Consume(queue2.Name, "consumer-2", false)
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
				log.Printf("Ack msg %s", msg.MessageId)

				return nil
			})

		}
	}()

	go func() {
		for message := range msgBus2 {
			msg := message

			g.Go(func() error {
				fmt.Printf("msg2 is %s \n", msg.Body)
				time.Sleep(5 * time.Second)
				if err := msg.Ack(false); err != nil {
					log.Printf("failed to ack the msg")
					return err
				}
				log.Printf("Ack msg %s", msg.MessageId)

				return nil
			})

		}
	}()

	select {}
}
