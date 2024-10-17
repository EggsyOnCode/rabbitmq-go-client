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

	res, err := client.Consume("customer_created", "consumer-1", false)
	if err != nil {
		panic(err)
	}

	go func() {
		for msg := range res {
			fmt.Printf("msg is %s \n", msg.Body)

			if !msg.Redelivered {
				msg.Nack(false, true)
				continue
			}

			if err := msg.Ack(false); err != nil {
				log.Printf("failed to ack teh msg")
				continue
			}

			log.Printf("Ack msg %s", msg.MessageId)
		}
	}()

	// creating ErrGrps to listen to msg using worker threads // multithreading using errgrp
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// 10 concurrent workers
	g.SetLimit(10)

	go func() {
		for message := range res {

			msg := message

			g.Go(func() error {
				fmt.Printf("msg is %s \n", msg.Body)
				time.Sleep(10 * time.Second)
				if err := msg.Ack(false); err != nil {
					log.Printf("failed to ack teh msg")
					return err
				}
				log.Printf("Ack msg %s", msg.MessageId)

				return nil
			})

		}
	}()

	select {}
}
