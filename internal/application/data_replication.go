package application

import (
	"context"
	"data-replication/config"
	"data-replication/internal/adapter"
	"data-replication/internal/domain/model"
	"data-replication/internal/enum/time_precision_mode"
	"data-replication/internal/infrastructure"
	"data-replication/internal/logger"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/samber/lo"
	"sync"
)

type DataReplicationService struct {
	wg        *sync.WaitGroup
	doneChan  map[string]chan bool
	instances map[string]DataReplicationInstance
}

type DataReplicationInstance struct {
	alias               string
	destinationDbConfig config.DbConfig // contain only key map
	sourceDbConfig      config.DbConfig // contain only key map
	debeziumKafkaConfig kafka.ConfigMap
	concurrencyConfig   config.CombineConcurrency
	debeziumKafka       *adapter.Consumer
	skipOp              []string
}

func NewDataReplicationService() *DataReplicationService {
	return &DataReplicationService{}
}

func (d *DataReplicationInstance) Load() {
	// init destination database
	desDb := infrastructure.Cfg.DestinationDbMap
	for _, dc := range desDb {
		d.alias = dc.Alias
		d.destinationDbConfig = dc
		if dc.Type == "postgres" {
			err := infrastructure.AddOrReplacePgPool(dc.Alias, dc, context.Background())
			if err != nil {
				logger.Errorf("Can't init destination pgPool %v", err)
				return
			}
		}
	}

	// init source database
	sourceDb := infrastructure.Cfg.SourceDbMap
	for _, sc := range sourceDb {
		d.sourceDbConfig = sc
		if sc.Type == "postgres" {
			err := infrastructure.AddOrReplacePgPool(sc.Alias, sc, context.Background())
			if err != nil {
				logger.Errorf("Can't init source pgPool %v", err)
				return
			}
		}
	}

}

func (d *DataReplicationInstance) RunModeOneToOne() {
	// start cdc one to one
	for _, mapSource := range infrastructure.Cfg.Cdc.Debezium.MappingSourceMap {
		if mapSource.IsFetchingSourceDbTable == true {
			// fetching table name
			sPool := infrastructure.GetPgPool(mapSource.SourceDb)
			tbs, err := sPool.GetAllTableNames(d.sourceDbConfig.Db)
			if err != nil {
				return
			}
			infrastructure.SetSourceDBOfCdc(mapSource.Name, tbs)
		}

		// each topic run singe
		if mapSource.CombineConcurrency.Status == false {
			for _, v := range mapSource.SourceDbTable {
				go d.StartConsumerTopic([]string{v})
			}
		} else {
			// start combine mode
			for _, v := range mapSource.CombineConcurrency.Combine {
				go d.StartConsumerTopic([]string{v})
			}
		}
	}
}

func (d *DataReplicationInstance) RunModeManyToOne() {

}

func (d *DataReplicationInstance) StartConsumerTopic(topic []string) {
	// init consumer
	c, err := adapter.NewConsumerWithConfig(&kafka.ConfigMap{
		"bootstrap.servers":     infrastructure.Cfg.Cdc.Debezium.Kafka.Servers,
		"group.id":              "data-replication.service",
		"max.poll.interval.ms":  86400000,
		"session.timeout.ms":    120000,
		"heartbeat.interval.ms": 5000,
		"enable.auto.commit":    infrastructure.Cfg.Cdc.Debezium.Kafka.AutoCommit, // commit manual
		"auto.offset.reset":     "earliest",
	})

	if err != nil {
		logger.Errorf("Can't create consumer %v", err)
	}

	d.debeziumKafka = c
	d.debeziumKafka.AutoCommit = infrastructure.Cfg.Cdc.Debezium.Kafka.AutoCommit
	d.debeziumKafka.CommitOnError = true

	c.SubscribeTopics(topic, d.HandlerMessage)
}

func (d *DataReplicationInstance) HandlerMessage(message *kafka.Message) error {
	// decode message
	var debeziumMessage model.DebeziumMessage
	var mockDebeziumValueMessage model.DebeziumValue
	var mockDebeziumKeyMessage model.DebeziumKey
	err := json.Unmarshal(message.Value, &mockDebeziumValueMessage)
	if err != nil {
		fmt.Printf("err 111")
		panic(err)
	}
	err = json.Unmarshal(message.Key, &mockDebeziumKeyMessage)
	if err != nil {
		fmt.Printf("err 222")
		panic(err)
	}

	debeziumMessage.Key = mockDebeziumKeyMessage
	debeziumMessage.Value = mockDebesziumValueMessage
	afterTable := debeziumMessage.GetSchemeAfterTable(time_precision_mode.Adaptive)

	// check skip message
	if lo.Contains(d.skipOp, afterTable.Operator) {
		return nil
	}

	switch afterTable.Operator {

	}

	return nil
}
