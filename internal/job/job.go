package job

import (
	"data-replication/internal/infrastructure"
	"data-replication/internal/logger"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/samber/lo"
)

var (
	OneToOne  = "OneToOne"
	ManyToOne = "ManyToOne"
)

type DataReplication struct {
	DoneChan chan bool
	Name     string
}

type IDataReplication interface {
	Execute() error
}

func (j *DataReplication) Execute() error {
	debeziumConf := infrastructure.Cfg.Cdc.Debezium
	consumerConf := &kafka.ConfigMap{
		"bootstrap.servers":     debeziumConf.Kafka.Servers,
		"group.id":              infrastructure.Cfg.ServiceName,
		"max.poll.interval.ms":  86400000,
		"session.timeout.ms":    120000,
		"heartbeat.interval.ms": 5000,
		"auto.offset.reset":     "earliest",
		"enable.auto.commit":    debeziumConf.Kafka.AutoCommit,
	}

	for _, connector := range infrastructure.Cfg.Cdc.Debezium.MappingSourceMap {
		if connector.CheckCreate {
			// TODO call api to debezium check existed
		}

		// create consumer
		consumer, err := infrastructure.NewConsumerWithConfig(consumerConf)
		if err != nil {
			logger.Errorf("Can't create consumer %v", err)
			return err
		}

		topics := lo.Map[string](connector.SourceDbTable, func(table string, _ int) string {
			return fmt.Sprintf("%v.%v.%v", connector.Prefix, connector.Schema, table)
		})

		consumer.SubscribeTopics(topics, j.HandlerMessage)

	}
	return nil
}

func newDataReplicationService(name string) IDataReplication {
	return &DataReplication{}
}
