package model

import (
	"data-replication/internal/enum/time_precision_mode"
	"encoding/json"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
)

func TestMessage(t *testing.T) {
	Convey("Testing real message with updated = nil", t, func() {
		var debeziumMessage DebeziumMessage[any]
		mockRawKeyMessage := "{\n  \"schema\": {\n    \"type\": \"struct\",\n    \"fields\": [\n      {\n        \"type\": \"int64\",\n        \"optional\": false,\n        \"field\": \"employee_id\"\n      }\n    ],\n    \"optional\": false,\n    \"name\": \"employees_pg.employees.temp.Key\"\n  },\n  \"payload\": {\n    \"employee_id\": 1\n  }\n}"
		mockRawValueMessage := "{ \"schema\": { \"type\": \"struct\", \"fields\": [ { \"type\": \"struct\", \"fields\": [ { \"type\": \"int64\", \"optional\": false, \"field\": \"employee_id\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"amount\" }, { \"type\": \"int32\", \"optional\": false, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"from_date\" }, { \"type\": \"int32\", \"optional\": false, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"to_date\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"default\": 0, \"field\": \"created_at\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"updated_at\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"deleted_at\" } ], \"optional\": true, \"name\": \"employees_pg.employees.temp.Value\", \"field\": \"before\" }, { \"type\": \"struct\", \"fields\": [ { \"type\": \"int64\", \"optional\": false, \"field\": \"employee_id\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"amount\" }, { \"type\": \"int32\", \"optional\": false, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"from_date\" }, { \"type\": \"int32\", \"optional\": false, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"to_date\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"default\": 0, \"field\": \"created_at\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"updated_at\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"deleted_at\" } ], \"optional\": true, \"name\": \"employees_pg.employees.temp.Value\", \"field\": \"after\" }, { \"type\": \"struct\", \"fields\": [ { \"type\": \"string\", \"optional\": false, \"field\": \"version\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"connector\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"name\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"ts_ms\" }, { \"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Enum\", \"version\": 1, \"parameters\": { \"allowed\": \"true,last,false,incremental\" }, \"default\": \"false\", \"field\": \"snapshot\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"db\" }, { \"type\": \"string\", \"optional\": true, \"field\": \"sequence\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_us\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_ns\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"schema\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"table\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"txId\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"lsn\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"xmin\" } ], \"optional\": false, \"name\": \"io.debezium.connector.postgresql.Source\", \"field\": \"source\" }, { \"type\": \"struct\", \"fields\": [ { \"type\": \"string\", \"optional\": false, \"field\": \"id\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"total_order\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"data_collection_order\" } ], \"optional\": true, \"name\": \"event.block\", \"version\": 1, \"field\": \"transaction\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"op\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_ms\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_us\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_ns\" } ], \"optional\": false, \"name\": \"employees_pg.employees.temp.Envelope\", \"version\": 2 }, \"payload\": { \"before\": null, \"after\": { \"employee_id\": 1, \"amount\": 1, \"from_date\": 6020, \"to_date\": 6022, \"created_at\": 1737366944918656, \"updated_at\": null, \"deleted_at\": null }, \"source\": { \"version\": \"3.0.0.Final\", \"connector\": \"postgresql\", \"name\": \"employees_pg\", \"ts_ms\": 1737366944920, \"snapshot\": \"false\", \"db\": \"employees\", \"sequence\": \"[\\\"280598296\\\",\\\"280598632\\\"]\", \"ts_us\": 1737366944920305, \"ts_ns\": 1737366944920305000, \"schema\": \"employees\", \"table\": \"temp\", \"txId\": 599, \"lsn\": 280598632, \"xmin\": null }, \"transaction\": null, \"op\": \"c\", \"ts_ms\": 1737366946745, \"ts_us\": 1737366946745142, \"ts_ns\": 1737366946745142623 } }"
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

		Convey("Test code after data", func() {
			for k, v := range *mockDebeziumValueMessage.Payload.After {
				fmt.Printf("%v - %v \n", k, v)
			}
		})

		Convey("Test function", func() {
			result := debeziumMessage.GetSchemeAfterTable(time_precision_mode.Adaptive)

			fmt.Printf("result.kafka: %v\n", result.ColWithKafka)
			fmt.Printf("result.debezium: %v\n", result.ColWithDebezium)
			tbPg := newPgTable(result)
			//query := tbPg.GenQueryCreateTable()
			//query := tbPg.GenQueryInsertInto()
			//query := tbPg.GenQueryUpdate()
			query := tbPg.GenQueryUpdateWithUpdateAt()

			fmt.Printf("query: %v\n", query)
		})
	})

	Convey("Testing real message with updated != nil", t, func() {
		var debeziumMessage DebeziumMessage[any]
		mockRawKeyMessage := "{\n  \"schema\": {\n    \"type\": \"struct\",\n    \"fields\": [\n      {\n        \"type\": \"int64\",\n        \"optional\": false,\n        \"field\": \"employee_id\"\n      }\n    ],\n    \"optional\": false,\n    \"name\": \"employees_pg.employees.temp.Key\"\n  },\n  \"payload\": {\n    \"employee_id\": 1\n  }\n}"
		mockRawValueMessage := "{ \"schema\": { \"type\": \"struct\", \"fields\": [ { \"type\": \"struct\", \"fields\": [ { \"type\": \"int64\", \"optional\": false, \"field\": \"employee_id\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"amount\" }, { \"type\": \"int32\", \"optional\": false, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"from_date\" }, { \"type\": \"int32\", \"optional\": false, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"to_date\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"default\": 0, \"field\": \"created_at\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"updated_at\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"deleted_at\" } ], \"optional\": true, \"name\": \"employees_pg.employees.temp.Value\", \"field\": \"before\" }, { \"type\": \"struct\", \"fields\": [ { \"type\": \"int64\", \"optional\": false, \"field\": \"employee_id\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"amount\" }, { \"type\": \"int32\", \"optional\": false, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"from_date\" }, { \"type\": \"int32\", \"optional\": false, \"name\": \"io.debezium.time.Date\", \"version\": 1, \"field\": \"to_date\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"default\": 0, \"field\": \"created_at\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"updated_at\" }, { \"type\": \"int64\", \"optional\": true, \"name\": \"io.debezium.time.MicroTimestamp\", \"version\": 1, \"field\": \"deleted_at\" } ], \"optional\": true, \"name\": \"employees_pg.employees.temp.Value\", \"field\": \"after\" }, { \"type\": \"struct\", \"fields\": [ { \"type\": \"string\", \"optional\": false, \"field\": \"version\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"connector\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"name\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"ts_ms\" }, { \"type\": \"string\", \"optional\": true, \"name\": \"io.debezium.data.Enum\", \"version\": 1, \"parameters\": { \"allowed\": \"true,last,false,incremental\" }, \"default\": \"false\", \"field\": \"snapshot\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"db\" }, { \"type\": \"string\", \"optional\": true, \"field\": \"sequence\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_us\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_ns\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"schema\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"table\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"txId\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"lsn\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"xmin\" } ], \"optional\": false, \"name\": \"io.debezium.connector.postgresql.Source\", \"field\": \"source\" }, { \"type\": \"struct\", \"fields\": [ { \"type\": \"string\", \"optional\": false, \"field\": \"id\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"total_order\" }, { \"type\": \"int64\", \"optional\": false, \"field\": \"data_collection_order\" } ], \"optional\": true, \"name\": \"event.block\", \"version\": 1, \"field\": \"transaction\" }, { \"type\": \"string\", \"optional\": false, \"field\": \"op\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_ms\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_us\" }, { \"type\": \"int64\", \"optional\": true, \"field\": \"ts_ns\" } ], \"optional\": false, \"name\": \"employees_pg.employees.temp.Envelope\", \"version\": 2 }, \"payload\": { \"before\": null, \"after\": { \"employee_id\": 1, \"amount\": 10, \"from_date\": 6020, \"to_date\": 6022, \"created_at\": 1737366944918656, \"updated_at\": 1737366980968443, \"deleted_at\": null }, \"source\": { \"version\": \"3.0.0.Final\", \"connector\": \"postgresql\", \"name\": \"employees_pg\", \"ts_ms\": 1737366980969, \"snapshot\": \"false\", \"db\": \"employees\", \"sequence\": \"[\\\"280598928\\\",\\\"280598984\\\"]\", \"ts_us\": 1737366980969371, \"ts_ns\": 1737366980969371000, \"schema\": \"employees\", \"table\": \"temp\", \"txId\": 600, \"lsn\": 280598984, \"xmin\": null }, \"transaction\": null, \"op\": \"u\", \"ts_ms\": 1737366981041, \"ts_us\": 1737366981041436, \"ts_ns\": 1737366981041436804 } }"
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

		Convey("Test function", func() {
			result := debeziumMessage.GetSchemeAfterTable(time_precision_mode.Adaptive)

			fmt.Printf("result.kafka: %v\n", result.ColWithKafka)
			fmt.Printf("result.debezium: %v\n", result.ColWithDebezium)
			tbPg := newPgTable(result)
			//query := tbPg.GenQueryCreateTable()
			//query := tbPg.GenQueryInsertInto()
			//query := tbPg.GenQueryUpdate()
			query := tbPg.GenQueryUpdateWithUpdateAt()

			fmt.Printf("query: %v\n", query)
		})
	})
}

func TestMessageMicroTime(t *testing.T) {
	Convey("Testing message MicroTime", t, func() {
		var debeziumMessage DebeziumMessage[any]
		mockRawKeyMessage := "{\n  \"schema\": {\n    \"type\": \"struct\",\n    \"fields\": [\n      {\n        \"type\": \"int64\",\n        \"optional\": false,\n        \"field\": \"employee_id\"\n      }\n    ],\n    \"optional\": false,\n    \"name\": \"employees_pg.employees.temp.Key\"\n  },\n  \"payload\": {\n    \"employee_id\": 1\n  }\n}"
		mockRawValueMessage := "{\n  \"schema\": {\n    \"type\": \"struct\",\n    \"fields\": [\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"employee_id\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"amount\"\n          },\n          {\n            \"type\": \"int32\",\n            \"optional\": false,\n            \"name\": \"io.debezium.time.Date\",\n            \"version\": 1,\n            \"field\": \"from_date\"\n          },\n          {\n            \"type\": \"int32\",\n            \"optional\": false,\n            \"name\": \"io.debezium.time.Date\",\n            \"version\": 1,\n            \"field\": \"to_date\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.MicroTimestamp\",\n            \"version\": 1,\n            \"default\": 0,\n            \"field\": \"created_at\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.MicroTimestamp\",\n            \"version\": 1,\n            \"field\": \"updated_at\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.MicroTimestamp\",\n            \"version\": 1,\n            \"field\": \"deleted_at\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.MicroTime\",\n            \"version\": 1,\n            \"field\": \"time_micro_test\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.Timestamp\",\n            \"version\": 1,\n            \"field\": \"timestamp2_test\"\n          }\n        ],\n        \"optional\": true,\n        \"name\": \"employees_pg.employees.temp.Value\",\n        \"field\": \"before\"\n      },\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"employee_id\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"amount\"\n          },\n          {\n            \"type\": \"int32\",\n            \"optional\": false,\n            \"name\": \"io.debezium.time.Date\",\n            \"version\": 1,\n            \"field\": \"from_date\"\n          },\n          {\n            \"type\": \"int32\",\n            \"optional\": false,\n            \"name\": \"io.debezium.time.Date\",\n            \"version\": 1,\n            \"field\": \"to_date\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.MicroTimestamp\",\n            \"version\": 1,\n            \"default\": 0,\n            \"field\": \"created_at\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.MicroTimestamp\",\n            \"version\": 1,\n            \"field\": \"updated_at\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.MicroTimestamp\",\n            \"version\": 1,\n            \"field\": \"deleted_at\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.MicroTime\",\n            \"version\": 1,\n            \"field\": \"time_micro_test\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"name\": \"io.debezium.time.Timestamp\",\n            \"version\": 1,\n            \"field\": \"timestamp2_test\"\n          }\n        ],\n        \"optional\": true,\n        \"name\": \"employees_pg.employees.temp.Value\",\n        \"field\": \"after\"\n      },\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"version\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"connector\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"name\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"ts_ms\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": true,\n            \"name\": \"io.debezium.data.Enum\",\n            \"version\": 1,\n            \"parameters\": {\n              \"allowed\": \"true,last,false,incremental\"\n            },\n            \"default\": \"false\",\n            \"field\": \"snapshot\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"db\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": true,\n            \"field\": \"sequence\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"ts_us\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"ts_ns\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"schema\"\n          },\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"table\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"txId\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"lsn\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": true,\n            \"field\": \"xmin\"\n          }\n        ],\n        \"optional\": false,\n        \"name\": \"io.debezium.connector.postgresql.Source\",\n        \"field\": \"source\"\n      },\n      {\n        \"type\": \"struct\",\n        \"fields\": [\n          {\n            \"type\": \"string\",\n            \"optional\": false,\n            \"field\": \"id\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"total_order\"\n          },\n          {\n            \"type\": \"int64\",\n            \"optional\": false,\n            \"field\": \"data_collection_order\"\n          }\n        ],\n        \"optional\": true,\n        \"name\": \"event.block\",\n        \"version\": 1,\n        \"field\": \"transaction\"\n      },\n      {\n        \"type\": \"string\",\n        \"optional\": false,\n        \"field\": \"op\"\n      },\n      {\n        \"type\": \"int64\",\n        \"optional\": true,\n        \"field\": \"ts_ms\"\n      },\n      {\n        \"type\": \"int64\",\n        \"optional\": true,\n        \"field\": \"ts_us\"\n      },\n      {\n        \"type\": \"int64\",\n        \"optional\": true,\n        \"field\": \"ts_ns\"\n      }\n    ],\n    \"optional\": false,\n    \"name\": \"employees_pg.employees.temp.Envelope\",\n    \"version\": 2\n  },\n  \"payload\": {\n    \"before\": null,\n    \"after\": {\n      \"employee_id\": 1,\n      \"amount\": 10,\n      \"from_date\": 6020,\n      \"to_date\": 6022,\n      \"created_at\": 1737366944918656,\n      \"updated_at\": 1737429410784492,\n      \"deleted_at\": null,\n      \"time_micro_test\": 64215000000,\n      \"timestamp2_test\": 1737454583000\n    },\n    \"source\": {\n      \"version\": \"3.0.0.Final\",\n      \"connector\": \"postgresql\",\n      \"name\": \"employees_pg\",\n      \"ts_ms\": 1737429410791,\n      \"snapshot\": \"false\",\n      \"db\": \"employees\",\n      \"sequence\": \"[\\\"280658120\\\",\\\"280658656\\\"]\",\n      \"ts_us\": 1737429410791977,\n      \"ts_ns\": 1737429410791977000,\n      \"schema\": \"employees\",\n      \"table\": \"temp\",\n      \"txId\": 615,\n      \"lsn\": 280658656,\n      \"xmin\": null\n    },\n    \"transaction\": null,\n    \"op\": \"u\",\n    \"ts_ms\": 1737429413437,\n    \"ts_us\": 1737429413437403,\n    \"ts_ns\": 1737429413437403814\n  }\n}"
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

		Convey("Test function", func() {
			result := debeziumMessage.GetSchemeAfterTable(time_precision_mode.Adaptive)

			fmt.Printf("result.kafka: %v\n", result.ColWithKafka)
			fmt.Printf("result.debezium: %v\n", result.ColWithDebezium)
			tbPg := newPgTable(result)
			//query := tbPg.GenQueryCreateTable()
			//query := tbPg.GenQueryInsertInto()
			//query := tbPg.GenQueryUpdate()
			query := tbPg.GenQueryUpdateWithUpdateAt()

			fmt.Printf("query: %v\n", query)
		})
	})
}
