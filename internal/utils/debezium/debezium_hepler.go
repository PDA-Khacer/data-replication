package utils

import "data-replication/config"

// TODO support ez build filter
type IDebeziumCommandBuilder interface {
	AddName(string)
	AddConnector(source config.MappingSource)
}

type DebeziumCommandBuilder struct {
	Command string
}

func (d *DebeziumCommandBuilder) AddName(s string) {
	//TODO implement me
	panic("implement me")
}

func (d *DebeziumCommandBuilder) AddConnector(source config.MappingSource) {
	//TODO implement me
	panic("implement me")
}

func NewDebeziumCommandBuilder() IDebeziumCommandBuilder {
	return &DebeziumCommandBuilder{}
}
