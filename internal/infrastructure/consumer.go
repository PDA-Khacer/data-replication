package infrastructure

import (
	"data-replication/internal/adapter"
	"data-replication/internal/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var MapConsumer map[string]*adapter.Consumer

func init() {
	MapConsumer = make(map[string]*adapter.Consumer)
}

func AddOrConsumer(key string, config kafka.ConfigMap) error {
	c, err := adapter.NewConsumerWithConfig(&config)
	if err != nil {
		logger.Errorf("NewConsumerWithConfig err %v", err)
		return err
	}
	MapConsumer[key] = c
	return nil
}

func GetConsumer(key string) *adapter.Consumer {
	return MapConsumer[key]
}

func RemoveConsumer(key string) error {
	if conn, ok := MapConsumer[key]; ok {
		conn.Close()
		delete(MapConsumer, key)
	}
	return nil
}
