package model

import (
	"encoding/json"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
)

func TestMessage(t *testing.T) {
	Convey("Testing real message", t, func() {
		mockRawMessage := "{\n  \"schema\": {\n    \"type\": \"struct\",\n    \"fields\": [\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"id\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"dept_name\"\n          }\n        ],\n        \"optional\": true,\n        \"name\": \"employees_pg_full.employees.department.Value\",\n        \"field\": \"before\"\n      },\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"id\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"dept_name\"\n          }\n        ],\n        \"optional\": true,\n        \"name\": \"employees_pg_full.employees.department.Value\",\n        \"field\": \"after\"\n      },\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"version\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"connector\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"name\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"ts_ms\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": true,\n            \"name\": \"io.debezium.data.Enum\",\n            \"version\": 1,\n            \"parameters\": {\n              \"allowed\": \"true,last,false,incremental\"\n            },\n            \"default\": \"false\",\n            \"field\": \"snapshot\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"db\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": true,\n            \"field\": \"sequence\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"ts_us\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"ts_ns\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"schema\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"table\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"txId\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"lsn\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"xmin\"\n          }\n        ],\n        \"optional\": false,\n        \"name\": \"io.debezium.connector.postgresql.Source\",\n        \"field\": \"source\"\n      },\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"id\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"total_order\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"data_collection_order\"\n          }\n        ],\n        \"optional\": true,\n        \"name\": \"event.block\",\n        \"version\": 1,\n        \"field\": \"transaction\"\n      },\n      {\n        \"type\": \"string\",\n        \"optional\": false,\n        \"field\": \"op\"\n      },\n      {\n        \"type\": \"int64\",\n        \"optional\": true,\n        \"field\": \"ts_ms\"\n      },\n      {\n        \"type\": \"int64\",\n        \"optional\": true,\n        \"field\": \"ts_us\"\n      },\n      {\n        \"type\": \"int64\",\n        \"optional\": true,\n        \"field\": \"ts_ns\"\n      }\n    ],\n    \"optional\": false,\n    \"name\": \"employees_pg_full.employees.department.Envelope\",\n    \"version\": 2\n  },\n  \"payload\": {\n    \"before\": null,\n    \"after\": {\n      \"id\": \"d009\",\n      \"dept_name\": \"Customer Service\"\n    },\n    \"source\": {\n      \"version\": \"3.0.0.Final\",\n      \"connector\": \"postgresql\",\n      \"name\": \"employees_pg_full\",\n      \"ts_ms\": 1736150506680,\n      \"snapshot\": \"first\",\n      \"db\": \"employees\",\n      \"sequence\": \"[null,\\\"279991904\\\"]\",\n      \"ts_us\": 1736150506680281,\n      \"ts_ns\": 1736150506680281000,\n      \"schema\": \"employees\",\n      \"table\": \"department\",\n      \"txId\": 549,\n      \"lsn\": 279991904,\n      \"xmin\": null\n    },\n    \"transaction\": null,\n    \"op\": \"r\",\n    \"ts_ms\": 1736150506958,\n    \"ts_us\": 1736150506958453,\n    \"ts_ns\": 1736150506958453356\n  }\n}"
		var mockDebeziumMessage DebeziumValue[any]
		err := json.Unmarshal([]byte(mockRawMessage), &mockDebeziumMessage)
		if err != nil {
			fmt.Printf("err 111")
			panic(err)
		}

		Convey("Test code DebeziumValue to Schema After", func() {
			after := SchemaTable{}

			for _, field := range mockDebeziumMessage.Schema.Fields {
				// get only after field
				if field.Field == "after" {
					// get schema & table form name with format: <Prefix>.<Schema>.<Table>.Value
					temp := strings.Split(field.Name, ".")
					// Careful Prefix maybe have dot.
					after.Schema = temp[len(temp)-3]
					after.TableName = temp[len(temp)-2]
					after.Prefix = strings.Join(temp[:len(temp)-3], ".")
					fmt.Println(after.Schema)
					fmt.Println(after.TableName)
					fmt.Println(after.Prefix)
					after.Col = map[string]string{}
					for _, col := range field.Fields {
						fmt.Printf("col %s : %s\n", col.Field, col.Type)
						after.Col[col.Field] = col.Type
					}
					break
				}
			}
		})
	})
}
