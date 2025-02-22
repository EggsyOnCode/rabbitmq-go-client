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
	conn, err := internal.CreateRmqpConnection("xen", "secret", "localhost:5671", "customers",
		"/home/xen/Desktop/code/event_driven/event-driven-rmq/tls-gen/basic/result/ca_certificate.pem",
		"/home/xen/Desktop/code/event_driven/event-driven-rmq/tls-gen/basic/result/client_xen-333_certificate.pem",
		"/home/xen/Desktop/code/event_driven/event-driven-rmq/tls-gen/basic/result/client_xen-333_key.pem",
	)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// cretaing another conn for consuming callbacks

	consumeCallback, err := internal.CreateRmqpConnection("xen", "secret", "localhost:5671", "customers",
		"/home/xen/Desktop/code/event_driven/event-driven-rmq/tls-gen/basic/result/ca_certificate.pem",
		"/home/xen/Desktop/code/event_driven/event-driven-rmq/tls-gen/basic/result/client_xen-333_certificate.pem",
		"/home/xen/Desktop/code/event_driven/event-driven-rmq/tls-gen/basic/result/client_xen-333_key.pem",
	)
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

			// Acknowledge the message
			if err := msg.Ack(false); err != nil {
				log.Printf("failed to ack the msg")
			}
		}
	}()

	// apply QOS
	// server can only send out 10 unack msg at a time
	if err := client.ApplyQoS(10, 0, true); err != nil {
		panic(err)
	}

	// creating bg ctx fro the msg payload
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		// since its a fanout exchange now we don't care about the routingKey
		if err := client.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp091.Persistent,
			// reply To is teh queue where the server will send back the response callbacks
			ReplyTo: queue.Name,
			// correlation id is used to match the response to the request ; a unique ID
			CorrelationId: fmt.Sprintf("correlation-id-%d", i),
			Body:          []byte("Persistent msg"),
		}); err != nil {
			panic(err)
		}
	}

	select {}
}
