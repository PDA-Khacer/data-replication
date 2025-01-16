package constants

var (
	// PG data type mapping https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-data-types

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

	GeometryDebeziumType     = "io.debezium.data.geometry.Geometry"
	GeographyDebeziumType    = "io.debezium.data.geometry.Geography"
	DoubleVectorDebeziumType = "io.debezium.data.DoubleVector"
	FloatVectorDebeziumType  = "io.debezium.data.FloatVector"
	SparseVectorDebeziumType = "io.debezium.data.SparseVector"
)

var UpdatedAtField = "updated_at"
var DeletedAtField = "deleted_at"
