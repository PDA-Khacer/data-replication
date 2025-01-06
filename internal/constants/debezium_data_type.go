package constants

var (
	// PG data type mapping https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-data-types

	DateDebeziumType              = "io.debezium.time.Date"
	TimeDebeziumType              = "io.debezium.time.Time"
	MicroTimeDebeziumType         = "io.debezium.time.MicroTime"
	TimestampDebeziumType         = "io.debezium.time.Timestamp"
	MicroTimestampDebeziumType    = "io.debezium.time.MicroTimestamp"
	EnumDebeziumType              = "io.debezium.data.Enum"
	BitsDebeziumType              = "io.debezium.data.Bits"
	ZonedTimestampDebeziumType    = "io.debezium.time.ZonedTimestamp"
	ZonedTimeDebeziumType         = "io.debezium.time.ZonedTime"
	MicroDurationTimeDebeziumType = "io.debezium.time.MicroDuration"
	IntervalTimeDebeziumType      = "io.debezium.time.Interval"
	JsonDebeziumType              = "io.debezium.data.Json"
	XmlDebeziumType               = "io.debezium.data.Xml"
	UuidDebeziumType              = "io.debezium.data.Uuid"
	PointDebeziumType             = "io.debezium.data.geometry.Point"
	LtreeDebeziumType             = "io.debezium.data.Ltree"

	DateKafkaDebeziumType            = "org.apache.kafka.connect.data.Date"
	TimeKafkaDebeziumType            = "org.apache.kafka.connect.data.Time"
	TimestampKafkaDebeziumType       = "org.apache.kafka.connect.data.Timestamp"
	DecimalKafkaDebeziumType         = "org.apache.kafka.connect.data.Decimal"
	VariableScaleDecimalDebeziumType = "io.debezium.data.VariableScaleDecimal"
	GeometryDebeziumType             = "io.debezium.data.geometry.Geometry"
	GeographyDebeziumType            = "io.debezium.data.geometry.Geography"
	DoubleVectorDebeziumType         = "io.debezium.data.DoubleVector"
	FloatVectorDebeziumType          = "io.debezium.data.FloatVector"
	SparseVectorDebeziumType         = "io.debezium.data.SparseVector"
)
