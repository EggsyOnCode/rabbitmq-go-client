package main

import (
	"context"
	"log"
	"time"

	"github.com/EggysOnCode/event-driven-rmq/internal"
	"github.com/rabbitmq/amqp091-go"
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

	log.Printf("Producer running....")

	// // creating a queue
	// if err = client.CreateQueue("customer_created", true, false); err != nil {
	// 	panic(err)
	// }
	// if err = client.CreateQueue("customer_logins", true, false); err != nil {
	// 	panic(err)
	// }

	// // creating a binding
	// if err = client.CreateBinding("customer_created", "customers.created.*", "customer_events"); err != nil {
	// 	panic(err)
	// }

	// if err = client.CreateBinding("customer_logins", "customers.*", "customer_events"); err != nil {
	// 	panic(err)
	// }

	// creating bg ctx fro the msg payload
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		// since its a fanout exchange now we don't care about the routingKey
		if err := client.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp091.Persistent,
			Body:         []byte("Persistent msg"),
		}); err != nil {
			panic(err)
		}
	}

	select {}
}
