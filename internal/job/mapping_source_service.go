package job

import (
	"context"
	"data-replication/config"
	"data-replication/internal/domain/model"
	"data-replication/internal/infrastructure"
	"data-replication/internal/logger"
	"encoding/json"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/samber/lo"
)

type Connector struct {
	Config *config.MappingSource
}

type ConnectorService interface {
	HandlerMessage(message *kafka.Message) error
	CreateOperator(model.DebeziumValue[any]) error
	UpdateOperator() error
	DeleteOperator() error
}

func (c *Connector) HandlerMessage(message *kafka.Message) error {
	// decode message
	recordKey := message.Key
	recordValue := message.Value
	keyMap := model.DebeziumKey{}
	dataMap := model.DebeziumValue[any]{}
	err := json.Unmarshal(recordKey, &keyMap)
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(recordValue, &dataMap)
	if err != nil {
		panic(err)
	}

	switch dataMap.Payload.Op {
	case "c":
	case "u":
	case "d":

	}
	return nil
}

func (c *Connector) CreateOperator(m model.DebeziumValue[any]) error {
	// get pool of destination
	pool := infrastructure.GetPgPool(c.Config.DestinationDb)
	if pool == nil {
		// init pool
		configDesDb, exits := lo.Find[config.DbConfig](infrastructure.Cfg.DestinationDbMap, func(item config.DbConfig) bool {
			return item.Alias == c.Config.DestinationDb
		})
		if !exits {
			logger.Errorf("Not found config of desnitation db")
			return errors.New("not found config of destination db")
		}
		err := infrastructure.AddOrReplacePgPool(c.Config.DestinationDb, configDesDb, context.Background())
		if err != nil {
			logger.Errorf("Can't open pool")
			return err
		}
	}

	// gen sql insert data

	return nil
}

func (c *Connector) UpdateOperator() error {
	//TODO implement me
	panic("implement me")
}

func (c *Connector) DeleteOperator() error {
	//TODO implement me
	panic("implement me")
}

func NewMappingSourceService(c *config.MappingSource) ConnectorService {
	return &Connector{
		Config: c,
	}
}
