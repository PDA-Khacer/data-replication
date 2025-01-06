package infrastructure

import "data-replication/config"

type IDebezium interface {
	CheckValidate(body map[string]interface{}) (bool, error)
	CreateConnector(body map[string]interface{}) (bool, error)
}

type DebeziumCaller struct {
	Config *config.Debezium
}
