package adapter

import (
	"data-replication/internal/logger"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Consumer can have many instance => not create goble
type Consumer struct {
	Consumer      *kafka.Consumer
	SignalTerm    chan bool
	TimeoutPool   int // ms
	AutoCommit    bool
	CommitOnError bool

	Config *kafka.ConfigMap
}

type IConsumer interface {
	SubscribeTopics(topic []string, handler func(*kafka.Message) error)
	Commit() error
	Close() error
}

// SubscribeTopics remind will sub all topic
func (c *Consumer) SubscribeTopics(topic []string, handler func(*kafka.Message) error) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("%v", r)
			logger.Errorf("Having panic on SubscribeTopics %v", err)
		}
	}()

	err := c.Consumer.SubscribeTopics(topic, nil)
	if err != nil {
		logger.Errorf("Failed to subscribe to topics: %s", err.Error())
		return
	}

	run := true
	for run {
		select {
		case sig := <-c.SignalTerm:
			logger.Infof("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Consumer.Poll(c.TimeoutPool)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:

				// call handler
				err = handler(e)
				if err != nil {
					logger.Errorf("Error on handler message: %v", err.Error())
					c.Commit()
				}

				if !c.AutoCommit {
					c.Commit()
				}
			case kafka.Error:
				logger.Errorf("%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
					return
				}
				if !c.AutoCommit && c.CommitOnError {
					c.Commit()
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
	c.Consumer.Close()
}

func (c *Consumer) Commit() error {
	_, err := c.Consumer.Commit()
	if err != nil {
		logger.Errorf("Failed to commit: %s", err.Error())
		return err
	}
	logger.Debugf("Commited message")
	return nil
}

func (c *Consumer) Close() error {
	err := c.Consumer.Close()
	if err != nil {
		return err
	}
	return nil
}

func NewConsumer() *Consumer {
	return &Consumer{
		Consumer:      nil,
		SignalTerm:    make(chan bool),
		TimeoutPool:   100,
		AutoCommit:    true,
		CommitOnError: false,
		Config:        nil,
	}
}

func NewConsumerWithConfig(config *kafka.ConfigMap) (*Consumer, error) {
	c := &Consumer{
		Consumer:      nil,
		SignalTerm:    make(chan bool),
		TimeoutPool:   100,
		AutoCommit:    true,
		CommitOnError: false,
		Config:        config,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		logger.Errorf("Create NewConsumerWithConfig fail %v", err.Error())
		return nil, err
	}
	c.Consumer = consumer
	return c, nil
}
