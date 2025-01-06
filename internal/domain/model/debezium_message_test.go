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
		var debeziumMessage DebeziumMessage[any]
		mockRawKeyMessage := "{\n  \"schema\": {\n    \"type\": \"struct\",\n    \"fields\": [\n      {\n        \"type\": \"int64\",\n        \"optional\": false,\n        \"default\": 0,\n        \"field\": \"id\"\n      }\n    ],\n    \"optional\": false,\n    \"name\": \"employees_pg.employees.employee.Key\"\n  },\n  \"payload\": {\n    \"id\": 10002\n  }\n}"
		mockRawValueMessage := "{\n  \"schema\": {\n    \"type\": \"struct\",\n    \"fields\": [\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"default\": 0,\n            \"field\": \"id\"\n          },\n          {\n            \"type\": \"int32\",\n            \"optional\": false,\n            \"name\": \"io.debezium.time.Date\",\n            \"version\": 1,\n            \"field\": \"birth_date\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"first_name\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"last_name\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"name\": \"io.debezium.data.Enum\",\n            \"version\": 1,\n            \"parameters\": {\n              \"allowed\": \"M,F\"\n            },\n            \"field\": \"gender\"\n          },\n          {\n            \"type\": \"int32\",\n            \"optional\": false,\n            \"name\": \"io.debezium.time.Date\",\n            \"version\": 1,\n            \"field\": \"hire_date\"\n          }\n        ],\n        \"optional\": true,\n        \"name\": \"employees_pg.employees.employee.Value\",\n        \"field\": \"before\"\n      },\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"default\": 0,\n            \"field\": \"id\"\n          },\n          {\n            \"type\": \"int32\",\n            \"optional\": false,\n            \"name\": \"io.debezium.time.Date\",\n            \"version\": 1,\n            \"field\": \"birth_date\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"first_name\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"last_name\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"name\": \"io.debezium.data.Enum\",\n            \"version\": 1,\n            \"parameters\": {\n              \"allowed\": \"M,F\"\n            },\n            \"field\": \"gender\"\n          },\n          {\n            \"type\": \"int32\",\n            \"optional\": false,\n            \"name\": \"io.debezium.time.Date\",\n            \"version\": 1,\n            \"field\": \"hire_date\"\n          }\n        ],\n        \"optional\": true,\n        \"name\": \"employees_pg.employees.employee.Value\",\n        \"field\": \"after\"\n      },\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"version\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"connector\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"name\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"ts_ms\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": true,\n            \"name\": \"io.debezium.data.Enum\",\n            \"version\": 1,\n            \"parameters\": {\n              \"allowed\": \"true,last,false,incremental\"\n            },\n            \"default\": \"false\",\n            \"field\": \"snapshot\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"db\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": true,\n            \"field\": \"sequence\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"ts_us\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"ts_ns\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"schema\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"table\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"txId\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"lsn\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"xmin\"\n          }\n        ],\n        \"optional\": false,\n        \"name\": \"io.debezium.connector.postgresql.Source\",\n        \"field\": \"source\"\n      },\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"id\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"total_order\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"data_collection_order\"\n          }\n        ],\n        \"optional\": true,\n        \"name\": \"event.block\",\n        \"version\": 1,\n        \"field\": \"transaction\"\n      },\n      {\n        \"type\": \"string\",\n        \"optional\": false,\n        \"field\": \"op\"\n      },\n      {\n        \"type\": \"int64\",\n        \"optional\": true,\n        \"field\": \"ts_ms\"\n      },\n      {\n        \"type\": \"int64\",\n        \"optional\": true,\n        \"field\": \"ts_us\"\n      },\n      {\n        \"type\": \"int64\",\n        \"optional\": true,\n        \"field\": \"ts_ns\"\n      }\n    ],\n    \"optional\": false,\n    \"name\": \"employees_pg.employees.employee.Envelope\",\n    \"version\": 2\n  },\n  \"payload\": {\n    \"before\": null,\n    \"after\": {\n      \"id\": 10002,\n      \"birth_date\": -2039,\n      \"first_name\": \"Bezalel\",\n      \"last_name\": \"Simmelwww\",\n      \"gender\": \"F\",\n      \"hire_date\": 5803\n    },\n    \"source\": {\n      \"version\": \"3.0.0.Final\",\n      \"connector\": \"postgresql\",\n      \"name\": \"employees_pg\",\n      \"ts_ms\": 1735790798737,\n      \"snapshot\": \"false\",\n      \"db\": \"employees\",\n      \"sequence\": \"[\\\"279972120\\\",\\\"279980928\\\"]\",\n      \"ts_us\": 1735790798737809,\n      \"ts_ns\": 1735790798737809000,\n      \"schema\": \"employees\",\n      \"table\": \"employee\",\n      \"txId\": 546,\n      \"lsn\": 279980928,\n      \"xmin\": null\n    },\n    \"transaction\": null,\n    \"op\": \"u\",\n    \"ts_ms\": 1735790798958,\n    \"ts_us\": 1735790798958160,\n    \"ts_ns\": 1735790798958160431\n  }\n}"
		var mockDebeziumValueMessage DebeziumValue[any]
		var mockDebeziumKeyMessage DebeziumKey
		err := json.Unmarshal([]byte(mockRawValueMessage), &mockDebeziumValueMessage)
		if err != nil {
			fmt.Printf("err 111")
			panic(err)
		}

		err = json.Unmarshal([]byte(mockRawKeyMessage), &mockDebeziumKeyMessage)
		if err != nil {
			fmt.Printf("err 222")
			panic(err)
		}

		debeziumMessage.Key = mockDebeziumKeyMessage
		debeziumMessage.Value = mockDebeziumValueMessage

		Convey("Test code DebeziumValue to Schema After", func() {
			after := SchemaTable{}

			for _, field := range mockDebeziumValueMessage.Schema.Fields {
				// get only after field
				if field.Field == "after" {
					// get schema & table form name with format: <Prefix>.<Schema>.<Table>.Value
					temp := strings.Split(*field.Name, ".")
					// Careful Prefix maybe have dot.
					after.Schema = temp[len(temp)-3]
					after.TableName = temp[len(temp)-2]
					after.Prefix = strings.Join(temp[:len(temp)-3], ".")
					fmt.Println(after.Schema)
					fmt.Println(after.TableName)
					fmt.Println(after.Prefix)
					after.ColWithKafka = map[string]string{}
					for _, col := range field.Fields {
						fmt.Printf("col %s : %s\n", col.Field, col.Type)
						after.ColWithKafka[col.Field] = col.Type
					}
					break
				}
			}
		})

		Convey("Test function", func() {
			result := debeziumMessage.GetSchemeAfterTable()

			fmt.Printf("result.kafka: %v\n", result.ColWithKafka)
			fmt.Printf("result.debezium: %v\n", result.ColWithDebezium)
		})
	})
}
