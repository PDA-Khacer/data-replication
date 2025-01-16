package connect

import "data-replication/internal/enum"

var (
	Date      enum.DebeziumDataTypeTimeConnect = "org.apache.kafka.connect.data.Date"      // int32
	Time      enum.DebeziumDataTypeTimeConnect = "org.apache.kafka.connect.data.Time"      // int64
	Timestamp enum.DebeziumDataTypeTimeConnect = "org.apache.kafka.connect.data.Timestamp" // int64
)
