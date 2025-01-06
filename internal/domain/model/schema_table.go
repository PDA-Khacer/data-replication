package model

type ISchemaTable interface {
	GenQueryCreateTable() []string
}

type SchemaTable struct {
	DBType          string
	Prefix          string
	Schema          string
	TableName       string
	ColWithKafka    map[string]string // field name : types
	ColWithDebezium map[string]string
	PrimaryKey      []string
}

func (st *SchemaTable) GenQueryCreateTable() []string {
	panic("implement me pls")
}
