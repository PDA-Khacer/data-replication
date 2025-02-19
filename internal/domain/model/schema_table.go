package model

import "data-replication/internal/enum"

type ISchemaTable interface {
	MappingValueToCorrectType(key string, values any, op enum.OperatorSQL) string

	// Mode one thread no Updated field

	GenQueryCreateTable() []string
	GenQueryInsertInto() []string
	GenQueryUpdate() []string

	// GenQueryDeleteExactlySingleNode need all field match
	GenQueryDeleteExactlySingleNode() []string
	// GenQueryDeleteExactlyMultiNode updated_at greater than
	GenQueryDeleteExactlyMultiNode() []string

	GenQueryDeleteWithKeys() []string

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
	ValBefore         map[string]any // for delete
	PrimaryKey        []string
	ValPrimaryKey     map[string]any
	TimePrecisionMode enum.TimePrecisionMode
	Operator          string
}

func (st *SchemaTable) MappingValueToCorrectType(key string, values any, op enum.OperatorSQL) string {
	panic("implement me pls")
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

func (st *SchemaTable) GenQueryDeleteExactlySingleNode() []string {
	panic("implement me pls")
}

func (st *SchemaTable) GenQueryDeleteExactlyMultiNode() []string {
	panic("implement me pls")
}

func (st *SchemaTable) GenQueryDeleteWithKeys() []string {
	panic("implement me pls")
}

func (st *SchemaTable) GenQueryInsertIntoWithUpdateAt() []string {
	panic("implement me pls")
}

func (st *SchemaTable) GenQueryUpdateWithUpdateAt() []string {
	panic("implement me pls")
}
