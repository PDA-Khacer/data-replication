package adaptive

import "data-replication/internal/enum"

var (
	Date           enum.DebeziumDataTypeTimeAdaptive = "io.debezium.time.Date"           // int32
	Time           enum.DebeziumDataTypeTimeAdaptive = "io.debezium.time.Time"           // int32
	MicroTime      enum.DebeziumDataTypeTimeAdaptive = "io.debezium.time.MicroTime"      // int64
	Timestamp      enum.DebeziumDataTypeTimeAdaptive = "io.debezium.time.Timestamp"      // int64
	MicroTimestamp enum.DebeziumDataTypeTimeAdaptive = "io.debezium.time.MicroTimestamp" // int64
)
