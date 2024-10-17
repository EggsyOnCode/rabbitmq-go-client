package main

import (
	"fmt"

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

	res, err := client.Consume("customer_created", "consumer-1", true)
	if err != nil {
		panic(err)
	}

	go func() {
		for msg := range res {
			fmt.Printf("msg is %s \n", msg.Body)
		}
	}()

	select {}
}
