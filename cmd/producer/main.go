package main

import (
	"log"
	"time"

	"github.com/EggysOnCode/event-driven-rmq/internal"
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
	if err = client.CreateQueue("customer_created", true, false); err!=nil {
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

	time.Sleep(60 * time.Second)

	log.Println("Producer is shutting down")
}
