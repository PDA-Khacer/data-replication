package model

type SchemaTable struct {
	DBType     string
	Prefix     string
	Schema     string
	TableName  string
	Col        map[string]string // field name : types
	PrimaryKey []string
}
