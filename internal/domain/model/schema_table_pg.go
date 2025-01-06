package model

import (
	"fmt"
	"github.com/samber/lo"
	"strings"
)

var (
	CREATE_TABLE_TEMPLATE = `CREATE TABLE IF NOT EXISTS %[1]s (%[2]s)`
)

type TablePg struct {
	SchemaTable
}

func newPgTable() ISchemaTable {
	return &TablePg{
		SchemaTable: SchemaTable{},
	}
}

func (t *TablePg) GenQueryCreateTable() (result []string) {
	if len(t.PrimaryKey) > 1 {
		colList := lo.MapToSlice(t.ColWithKafka, func(col string, typeCol string) string {
			typePg := ""
			switch typeCol {
			case "int64":
				typePg = "bigint"
			case "int32":
				// check type debezium
				if t.ColWithDebezium[col] == "io.debezium.time.Date" {
					typePg = "date"
				} else {
					typePg = "numeric"
				}
			case "int":
				typePg = "numeric"
			case "string":
				typePg = "text"
			case "bool":
				typePg = "boolean"
			case "float":
				typePg = "numeric"
			case "double":
				typePg = "double precision"
				// TODO check time stamp
			}
			return fmt.Sprintf("%[1]s %[2]s", col, typePg)
		})
		result = append(result, strings.Join(colList, ", \n"))
	} else {

	}
	return
}
