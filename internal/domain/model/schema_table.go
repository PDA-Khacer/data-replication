package model

import "data-replication/internal/enum"

type ISchemaTable interface {
	// Mode one thread no Updated field

	GenQueryCreateTable() []string
	GenQueryInsertInto() []string
	GenQueryUpdate() []string

	// Mode have updated_at field

	GenQueryInsertIntoWithUpdateAt() []string
	GenQueryUpdateWithUpdateAt() []string
}

type SchemaTable struct {
	DBType            string
	Prefix            string
	Schema            string
	TableName         string
	ColWithKafka      map[string]string // field name : types
	ColWithDebezium   map[string]string
	ValAfter          map[string]any
	PrimaryKey        []string
	TimePrecisionMode enum.TimePrecisionMode
}

func (st *SchemaTable) GenQueryCreateTable() []string {
	panic("implement me pls")
}

func (st *SchemaTable) GenQueryInsertInto() []string {
	panic("implement me pls")
}

func (st *SchemaTable) GenQueryUpdate() []string {
	panic("implement me pls")
}

func (st *SchemaTable) GenQueryInsertIntoWithUpdateAt() []string {
	panic("implement me pls")
}

func (st *SchemaTable) GenQueryUpdateWithUpdateAt() []string {
	panic("implement me pls")
}
