package model

import (
	"data-replication/internal/enum"
	"github.com/samber/lo"
	"strings"
)

type DebeziumMessage struct {
	Key   DebeziumKey
	Value DebeziumValue
}

type DebeziumKey struct {
	Schema  Schema         `json:"schema"`
	Payload map[string]any `json:"payload"`
}

type Schema struct {
	Type       string                 `json:"type"`
	Fields     []Field                `json:"fields"`
	Optional   *bool                  `json:"optional"`
	Name       string                 `json:"name"`
	Parameters map[string]interface{} `json:"parameters"`
	Version    *int                   `json:"version"`
}

type Payload struct {
	Id          any             `json:"id"`
	Before      *map[string]any `json:"before"`
	After       *map[string]any `json:"after"`
	Source      Source          `json:"source"`
	Transaction any             `json:"transaction"`
	Op          string          `json:"op"`
	TsMs        int64           `json:"ts_ms"`
	TsUs        int64           `json:"ts_us"`
	TsNs        int64           `json:"ts_ns"`
}

type Field struct {
	Name   *string `json:"name"`
	Type   string  `json:"type"`
	Option bool    `json:"option"`
	// Field update data
	Field  string  `json:"field"`
	Fields []Field `json:"fields"`
}

type DebeziumValue struct {
	Schema  Schema  `json:"schema"`
	Payload Payload `json:"payload"`
}

type Source struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	TsMs      int64  `json:"ts_ms"`
	Snapshot  string `json:"snapshot"`
	Db        string `json:"db"`
	Sequence  string `json:"sequence"`
	TsUs      int64  `json:"ts_us"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	TxId      int    `json:"txId"`
	Lsn       int    `json:"lsn"`
	Xmin      any    `json:"xmin"`
}

// GetSchemeAfterTable Convert schema to SchemaTable
func (m *DebeziumMessage) GetSchemeAfterTable(mode enum.TimePrecisionMode) (after SchemaTable) {
	after.TimePrecisionMode = mode

	dk := m.Key
	// get private key
	after.PrimaryKey = lo.Map(dk.Schema.Fields, func(item Field, _ int) string {
		return item.Field
	})

	after.ValPrimaryKey = make(map[string]any)
	after.ValPrimaryKey = lo.Associate(dk.Schema.Fields, func(item Field) (string, any) {
		return item.Field, dk.Payload[item.Field]
	})

	dv := m.Value
	for _, field := range dv.Schema.Fields {
		// get only after field
		if field.Field == "after" && field.Name != nil {
			// get schema & table form name with format: <Prefix>.<Schema>.<Table>.Value
			temp := strings.Split(*field.Name, ".")
			// Careful Prefix maybe have dot.
			after.Schema = temp[len(temp)-3]
			after.TableName = temp[len(temp)-2]
			after.Prefix = strings.Join(temp[:len(temp)-3], ".")
			after.ColWithKafka = map[string]string{}
			after.ColWithDebezium = map[string]string{}
			for _, col := range field.Fields {
				after.ColWithKafka[col.Field] = col.Type
				if col.Name != nil {
					after.ColWithDebezium[col.Field] = *col.Name
				} else {
					after.ColWithDebezium[col.Field] = col.Type
				}
			}
			break
		}
	}
	// handler after data & before data
	after.ValAfter = make(map[string]any)
	if dv.Payload.After != nil {
		for key, value := range *dv.Payload.After {
			after.ValAfter[key] = value
		}
	}
	// TODO add option on config
	after.ValBefore = make(map[string]any)
	if dv.Payload.Before != nil {
		for key, value := range *dv.Payload.Before {
			after.ValBefore[key] = value
		}
	}
	return
}
