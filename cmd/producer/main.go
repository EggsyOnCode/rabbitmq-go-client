package main

import (
	"context"
	"fmt"
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

	// cretaing another conn for consuming callbacks

	consumeCallback, err := internal.CreateRmqpConnection("xen", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer consumeCallback.Close()

	// creating a client
	client := internal.NewRabbitMQClient(conn)
	defer client.Close()

	// creating a consume client
	consumeClient := internal.NewRabbitMQClient(consumeCallback)
	defer consumeClient.Close()

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

	// create a queue for receiving the callabck responses
	queue, err := consumeClient.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	// Bind the queue to the exchange
	if err := consumeClient.CreateBinding(queue.Name, queue.Name, "customer_callbacks"); err != nil {
		panic(err)
	}

	// start consuming
	go func() {
		msgBus, err := consumeClient.Consume(queue.Name, "customer-api", false)
		if err != nil {
			panic(err)
		}

		for msg := range msgBus {
			log.Printf("Received msg: %s wit coorelation Id %v", string(msg.Body), msg.CorrelationId)
		}
	}()

	// creating bg ctx fro the msg payload
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		// since its a fanout exchange now we don't care about the routingKey
		if err := client.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp091.Persistent,
			// reply To is teh queue where the server will send back the response callbacks
			ReplyTo:       queue.Name,
			// correlation id is used to match the response to the request ; a unique ID
			CorrelationId: fmt.Sprintf("correlation-id-%d", i),
			Body:          []byte("Persistent msg"),
		}); err != nil {
			panic(err)
		}
	}

	select {}
}
