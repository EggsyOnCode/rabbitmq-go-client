package internal

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	// TCP conncetion with rmqp server
	conn *amqp.Connection
	// channel is used to receive / produce messages
	ch *amqp.Channel
}

func CreateRmqpConnection(username, pwd, host, vhost string) (*amqp.Connection, error) {
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, pwd, host, vhost))
}

func NewRabbitMQClient(conn *amqp.Connection) *RabbitMQClient {
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	return &RabbitMQClient{
		conn: conn,
		ch:   ch,
	}
}

// close closes the one of hte multiplexed channels and not the underlying TCP connections
func (rc *RabbitMQClient) Close() error {
	return rc.ch.Close()
}

// a wrapper method around amqp.Channel.QueueDeclare to avoid exposing the channel directly to the user
func (rc *RabbitMQClient) CreateQueue(name string, durable, autodelete bool) error {
	_, err := rc.ch.QueueDeclare(name, durable, autodelete, false, false, nil)
	return err
}

// bind the queue with the specific binding key to an exchange
func (rc *RabbitMQClient) CreateBinding(queue_name, binding_key, exchange string) error {
	// leaving noWait to false, which will return an error if the channle fails to bind
	return rc.ch.QueueBind(queue_name, binding_key, exchange, false, nil)
}

// wrapper for publishing messages
func (rc *RabbitMQClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	return rc.ch.PublishWithContext(ctx,
		exchange,
		routingKey,
		// mandatory flag is for receiving an error if exchange encoutners a failure to send msg
		true,
		false,
		options,
	)
}

// wrapper for consuming msgs
func (rc *RabbitMQClient) Consume(queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(
		queue,
		consumer,
		// if the consuming microservice takes time to process the msgs and there's a possibiltiy of failure
		// better manually ACK then set autoAck because autoAck removes msg from queue as soon as its delivered 
		autoAck,
		// exclusive flag should be set to true only if there's only one consumer consuming from the queue, otherwise its set to false
		// and server will use load-balancing to distribute msgs
		false,
		false,
		false,
		nil,
	)
}
