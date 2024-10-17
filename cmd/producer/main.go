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

	// creating a queue
	if err = client.CreateQueue("customer_created", true, false); err != nil {
		panic(err)
	}
	if err = client.CreateQueue("customer_logins", true, false); err != nil {
		panic(err)
	}

	// creating a binding
	if err = client.CreateBinding("customer_created", "customers.created.*", "customer_events"); err != nil {
		panic(err)
	}

	if err = client.CreateBinding("customer_logins", "customers.*", "customer_events"); err != nil {
		panic(err)
	}

	// creating bg ctx fro the msg payload
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// sending some msgs B!
	if err := client.Send(ctx, "customer_events", "customers.created.pk", amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Persistent,
		Body:         []byte("a Persistent msg here guys!"),
	}); err != nil {
		panic(err)
	}


	if err := client.Send(ctx, "customer_events", "customers.logins", amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Transient,
		Body:         []byte("a Transient msg here guys!"),
	}); err != nil {
		panic(err)
	}

	time.Sleep(60 * time.Second)

	log.Println("Producer is shutting down")
}
