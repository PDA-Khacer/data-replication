package precise

import "data-replication/internal/enum"

var (
	Decimal              enum.DebeziumDataTypeDecimalPrecise = "org.apache.kafka.connect.data.Decimal"
	VariableScaleDecimal enum.DebeziumDataTypeDecimalPrecise = "io.debezium.data.VariableScaleDecimal"
)
