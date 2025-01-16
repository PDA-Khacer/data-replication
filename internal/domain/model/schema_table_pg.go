package model

import (
	"data-replication/internal/constants"
	"data-replication/internal/enum/debezium_time_datatype/adaptive"
	"data-replication/internal/enum/debezium_time_datatype/connect"
	"data-replication/internal/enum/time_precision_mode"
	utils "data-replication/internal/utils/data_type/pg"
	"fmt"
	"github.com/samber/lo"
	"strings"
)

var (
	CREATE_TABLE_TEMPLATE = `CREATE TABLE IF NOT EXISTS %[1]s (%[2]s)`
	INSERT_TABLE_TEMPLATE = `INSERT INTO %[1]s (%[2]s) VALUES (%[3]s)` // MAPPING 1-1
	UPDATE_TABLE_TEMPLATE = `UPDATE %[1]s SET %[2]s WHERE %[3]s`
)

type TablePg struct {
	SchemaTable
}

func newPgTable(st SchemaTable) ISchemaTable {
	return &TablePg{
		SchemaTable: st,
	}
}

// GenQueryCreateTable without private key
func (t *TablePg) GenQueryCreateTable() (result []string) {
	colList := lo.MapToSlice(t.ColWithKafka, func(col string, typeCol string) string {
		typePg := ""
		switch typeCol {
		case "int64":
			typePg = "bigint"
			switch t.TimePrecisionMode {
			case time_precision_mode.Adaptive:
				if t.ColWithDebezium[col] == string(adaptive.MicroTime) {
					typePg = "TIME(6)"
				} else if t.ColWithDebezium[col] == string(adaptive.Timestamp) || t.ColWithDebezium[col] == string(adaptive.MicroTimestamp) {
					typePg = "TIMESTAMP"
				}
			case time_precision_mode.AdaptiveTimeMicroseconds:
				if t.ColWithDebezium[col] == string(adaptive.MicroTime) {
					typePg = "TIME"
				} else if t.ColWithDebezium[col] == string(adaptive.Timestamp) || t.ColWithDebezium[col] == string(adaptive.MicroTimestamp) {
					typePg = "TIMESTAMP"
				}
			case time_precision_mode.Connect:
				if t.ColWithDebezium[col] == string(connect.Time) {
					typePg = "TIME"
				}
				if t.ColWithDebezium[col] == string(connect.Timestamp) {
					typePg = "TIMESTAMP"
				}
			}
			if t.ColWithDebezium[col] == constants.IntervalTimeDebeziumType {
				typePg = "INTERVAL"
			}
		case "int32":
			typePg = "numeric"
			// check type debezium
			switch t.TimePrecisionMode {
			case time_precision_mode.Adaptive:
				if t.ColWithDebezium[col] == string(adaptive.Date) {
					typePg = "DATE"
				} else if t.ColWithDebezium[col] == string(adaptive.Time) {
					typePg = "TIME"
				}
			case time_precision_mode.AdaptiveTimeMicroseconds:
				if t.ColWithDebezium[col] == string(adaptive.Date) {
					typePg = "DATE"
				}
			case time_precision_mode.Connect:
				if t.ColWithDebezium[col] == string(connect.Date) {
					typePg = "DATE"
				}
			}
		case "int":
			typePg = "numeric"
		case "string":
			typePg = "text"
			if t.ColWithDebezium[col] == constants.JsonDebeziumType {
				typePg = "JSONB"
			}
			if t.ColWithDebezium[col] == constants.XmlDebeziumType {
				typePg = "XML"
			}
			if t.ColWithDebezium[col] == constants.UuidDebeziumType {
				typePg = "UUID"
			}
			if t.ColWithDebezium[col] == constants.LtreeDebeziumType {
				typePg = "LTREE"
			}
			if t.ColWithDebezium[col] == constants.ZonedTimestampDebeziumType {
				typePg = "TIMESTAMPTZ"
			}
			if t.ColWithDebezium[col] == constants.ZonedTimeDebeziumType {
				typePg = "TIMETZ"
			}
			if t.ColWithDebezium[col] == constants.IntervalTimeDebeziumType {
				typePg = "INTERVAL"
			}
		case "bool":
			typePg = "boolean"
		case "float":
			typePg = "real"
		case "double":
			typePg = "double precision"
		}
		return fmt.Sprintf("%[1]s %[2]s", col, typePg)
	})
	result = append(result, fmt.Sprintf(CREATE_TABLE_TEMPLATE, fmt.Sprintf("%s.%s", t.Schema, t.TableName), strings.Join(colList, ", \n")))
	return
}

func (t *TablePg) GenQueryInsertInto() (result []string) {
	col := lo.Keys(t.ColWithKafka)

	vals := lo.Map(col, func(c string, _ int) string {
		var val string
		switch t.ColWithKafka[c] {
		case "int64", "int32", "int":
			if t.ColWithDebezium[c] == string(adaptive.MicroTime) ||
				t.ColWithDebezium[c] == string(adaptive.Time) ||
				t.ColWithDebezium[c] == string(connect.Time) {
				// convert to time only
				val = fmt.Sprintf(`'%s'`, utils.ConvertDebeziumTimeDateToTimeFloat64(t.ValAfter[c].(float64)).Format("15:04:05"))
			} else if t.ColWithDebezium[c] == string(adaptive.Timestamp) ||
				t.ColWithDebezium[c] == string(adaptive.MicroTimestamp) ||
				(t.ColWithDebezium[c] == string(connect.Timestamp)) {
				// convert to timestamp
				val = fmt.Sprintf(`'%s'`, utils.GetDateOnlyYMDTime(utils.ConvertDebeziumTimeDateToTimeFloat64(t.ValAfter[c].(float64))))
			} else if t.ColWithDebezium[c] == string(adaptive.Date) ||
				t.ColWithDebezium[c] == string(connect.Date) {
				val = fmt.Sprintf(`'%s'`, utils.GetDateOnlyYMD(utils.ConvertDebeziumTimeDateToTimeFloat64(t.ValAfter[c].(float64))))
			} else {
				val = fmt.Sprintf("%.0f", t.ValAfter[c].(float64))
			}
		case "string":
			val = fmt.Sprintf(`'%s'`, t.ValAfter[c].(string))
		case "bool":
			val = t.ValAfter[c].(string)
		case "float", "double":
			val = fmt.Sprintf("%f", t.ValAfter[c].(float64))
		}
		return val
	})
	result = append(result, fmt.Sprintf(INSERT_TABLE_TEMPLATE, fmt.Sprintf("%s.%s", t.Schema, t.TableName), strings.Join(col, ", "), strings.Join(vals, ", ")))
	return
}

func (t *TablePg) GenQueryUpdate() (result []string) {
	col := lo.Keys(t.ColWithKafka)
	var whereCon []string
	vals := lo.Map(col, func(c string, _ int) string {
		var val string
		switch t.ColWithKafka[c] {
		case "int64", "int32", "int":
			if t.ColWithDebezium[c] == string(adaptive.MicroTime) ||
				t.ColWithDebezium[c] == string(adaptive.Time) ||
				t.ColWithDebezium[c] == string(connect.Time) {
				// convert to time only
				val = fmt.Sprintf(`%s = '%s'`, c, utils.ConvertDebeziumTimeDateToTimeFloat64(t.ValAfter[c].(float64)).Format("15:04:05"))
			} else if t.ColWithDebezium[c] == string(adaptive.Timestamp) ||
				t.ColWithDebezium[c] == string(adaptive.MicroTimestamp) ||
				(t.ColWithDebezium[c] == string(connect.Timestamp)) {
				// convert to timestamp
				val = fmt.Sprintf(`%s = '%s'`, c, utils.GetDateOnlyYMDTime(utils.ConvertDebeziumTimeDateToTimeFloat64(t.ValAfter[c].(float64))))
			} else if t.ColWithDebezium[c] == string(adaptive.Date) ||
				t.ColWithDebezium[c] == string(connect.Date) {
				val = fmt.Sprintf(`%s = '%s'`, c, utils.GetDateOnlyYMD(utils.ConvertDebeziumTimeDateToTimeFloat64(t.ValAfter[c].(float64))))
			} else {
				val = fmt.Sprintf("%s = %.0f", c, t.ValAfter[c].(float64))
			}
		case "string":
			val = fmt.Sprintf(`%s = '%s'`, c, t.ValAfter[c].(string))
		case "bool":
			val = t.ValAfter[c].(string)
		case "float", "double":
			val = fmt.Sprintf("%s = %f", c, t.ValAfter[c].(float64))
		}
		if lo.Contains(t.PrimaryKey, c) {
			whereCon = append(whereCon, val)
			return ""
		}
		return val
	})
	result = append(result, fmt.Sprintf(UPDATE_TABLE_TEMPLATE,
		fmt.Sprintf("%s.%s", t.Schema, t.TableName),
		strings.Join(lo.Filter[string](vals,
			func(item string, _ int) bool {
				return item != ""
			}), " ,"), strings.Join(whereCon, " AND ")))
	return
}

func (t *TablePg) GenQueryUpdateWithUpdateAt() (result []string) {
	col := lo.Keys(t.ColWithKafka)
	var whereCon []string
	vals := lo.Map(col, func(c string, _ int) string {
		var val string
		switch t.ColWithKafka[c] {
		case "int64", "int32", "int":
			if t.ColWithDebezium[c] == string(adaptive.MicroTime) ||
				t.ColWithDebezium[c] == string(adaptive.Time) ||
				t.ColWithDebezium[c] == string(connect.Time) {
				// convert to time only
				val = fmt.Sprintf(`%s = '%s'`, c, utils.ConvertDebeziumTimeDateToTimeFloat64(t.ValAfter[c].(float64)).Format("15:04:05"))
			} else if t.ColWithDebezium[c] == string(adaptive.Timestamp) ||
				t.ColWithDebezium[c] == string(adaptive.MicroTimestamp) ||
				(t.ColWithDebezium[c] == string(connect.Timestamp)) {
				// convert to timestamp
				val = fmt.Sprintf(`%s = '%s'`, c, utils.GetDateOnlyYMDTime(utils.ConvertDebeziumTimeDateToTimeFloat64(t.ValAfter[c].(float64))))
			} else if t.ColWithDebezium[c] == string(adaptive.Date) ||
				t.ColWithDebezium[c] == string(connect.Date) {
				val = fmt.Sprintf(`%s = '%s'`, c, utils.GetDateOnlyYMD(utils.ConvertDebeziumTimeDateToTimeFloat64(t.ValAfter[c].(float64))))
			} else {
				val = fmt.Sprintf("%s = %.0f", c, t.ValAfter[c].(float64))
			}
		case "string":
			val = fmt.Sprintf(`%s = '%s'`, c, t.ValAfter[c].(string))
		case "bool":
			val = t.ValAfter[c].(string)
		case "float", "double":
			val = fmt.Sprintf("%s = %f", c, t.ValAfter[c].(float64))
		}
		if lo.Contains(t.PrimaryKey, c) {
			whereCon = append(whereCon, val)
			return ""
		}
		if c == constants.UpdatedAtField {
			whereCon = append(whereCon, strings.ReplaceAll(val, "=", "<="))
			return ""
		}
		return val
	})

	// add condition check deleted_at = false
	whereCon = append(whereCon, "deleted_at = false")

	result = append(result, fmt.Sprintf(UPDATE_TABLE_TEMPLATE,
		fmt.Sprintf("%s.%s", t.Schema, t.TableName),
		strings.Join(lo.Filter[string](vals,
			func(item string, _ int) bool {
				return item != ""
			}), " ,"), strings.Join(whereCon, " AND ")))
	return
}
