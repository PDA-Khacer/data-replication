package infrastructure

import (
	"data-replication/internal/domain/model"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestIntegrationTestConsumer(t *testing.T) {
	Convey("Testing real consumer", t, func() {
		c, err := NewConsumerWithConfig(&kafka.ConfigMap{
			"bootstrap.servers":     "localhost:9092",
			"group.id":              "test_integration",
			"max.poll.interval.ms":  86400000,
			"session.timeout.ms":    120000,
			"heartbeat.interval.ms": 5000,
			//"enable.auto.commit":    false, // commit manual
			"auto.offset.reset": "earliest",
		})

		if err != nil {
			panic(err)
		}
		go c.SubscribeTopics([]string{
			"employees_pg.employees.department",
			"employees_pg.employees.employee"}, func(message *kafka.Message) error {
			recordKey := message.Key
			recordValue := message.Value
			dataMap := model.DebeziumValue[any]{}
			err := json.Unmarshal(recordValue, &dataMap)
			if err != nil {
				panic(err)
			}
			fmt.Printf("recordKey %v \n", string(recordKey))
			fmt.Printf("recordValue %v \n", dataMap)

			return nil
		})

		time.Sleep(20 * time.Second)
	})
}
