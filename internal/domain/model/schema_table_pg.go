package model

import (
	"data-replication/internal/constants"
	"data-replication/internal/enum"
	"data-replication/internal/enum/debezium_time_datatype/adaptive"
	"data-replication/internal/enum/debezium_time_datatype/connect"
	operator "data-replication/internal/enum/operator_sql"
	"data-replication/internal/enum/time_precision_mode"
	"data-replication/internal/logger"
	utils "data-replication/internal/utils/data_type/pg"
	"fmt"
	"github.com/samber/lo"
	"strings"
)

var (
	CREATE_TABLE_TEMPLATE = `CREATE TABLE IF NOT EXISTS %[1]s (%[2]s)`
	INSERT_TABLE_TEMPLATE = `INSERT INTO %[1]s (%[2]s) VALUES (%[3]s)` // MAPPING 1-1
	UPDATE_TABLE_TEMPLATE = `UPDATE %[1]s SET %[2]s WHERE %[3]s`
	DELETE_TABLE_TEMPLATE = `DELETE FROM %[1]s WHERE %[2]s`
)

type TablePg struct {
	SchemaTable
}

func newPgTable(st SchemaTable) ISchemaTable {
	return &TablePg{
		SchemaTable: st,
	}
}

func (t *TablePg) MappingValueToCorrectType(key string, values any, op enum.OperatorSQL) string {
	if values == nil {
		return fmt.Sprintf(`%[1]s %[2]s null`, key, op)
	}

	var val string
	switch t.ColWithKafka[key] {
	case "int32":
		if t.ColWithDebezium[key] == string(adaptive.Date) || t.ColWithDebezium[key] == string(connect.Date) {
			// DATE type adaptive same with connect
			val = fmt.Sprintf(`%[1]s %[3]s '%[2]s'`, key, utils.GetDateOnlyYMD(utils.ConvertDebeziumTimeDateToTimeFloat64(values.(float64))), op)
		} else if t.ColWithDebezium[key] == string(adaptive.Time) || t.ColWithDebezium[key] == string(connect.Time) {
			// TIME(1), TIME(2), TIME(3) Represents the number of milliseconds past midnight, and does not include timezone information.
			val = fmt.Sprintf(`%[1]s %[3]s '%[2]s'`, key, utils.ConvertDebeziumTimeToTime(values.(float64)).Format("15:04:05"), op)
		} else {
			val = fmt.Sprintf("%[1]s %[3]s %.0[2]f", key, values.(float64), op)
		}
	case "int64":
		if t.ColWithDebezium[key] == string(adaptive.MicroTime) {
			// TIME(4), TIME(5), TIME(6) Represents the number of microseconds past midnight, and does not include timezone information.
			val = fmt.Sprintf(`%[1]s %[3]s '%[2]s'`, key, utils.ConvertDebeziumMircoTimeToTime(values.(float64)).Format("15:04:05"), op)
		} else if t.ColWithDebezium[key] == string(adaptive.Timestamp) || t.ColWithDebezium[key] == string(connect.Timestamp) {
			// TIMESTAMP(1), TIMESTAMP(2), TIMESTAMP(3) Represents the number of milliseconds since the epoch, and does not include timezone information.
			val = fmt.Sprintf(`%[1]s %[3]s '%[2]s'`, key, utils.GetDateOnlyYMDTime(utils.ConvertDebeziumMilliTimestampToTime(values.(float64))), op)
		} else if t.ColWithDebezium[key] == string(adaptive.MicroTimestamp) {
			// TIMESTAMP(4), TIMESTAMP(5), TIMESTAMP(6), TIMESTAMP
			val = fmt.Sprintf(`%[1]s %[3]s '%[2]s'`, key, utils.GetDateOnlyYMDTime(utils.ConvertDebeziumMicroTimestampToTime(values.(float64))), op)
		} else if t.ColWithDebezium[key] == string(adaptive.Date) || t.ColWithDebezium[key] == string(connect.Date) {
			val = fmt.Sprintf(`%[1]s %[3]s '%[2]s'`, key, utils.GetDateOnlyYMD(utils.ConvertDebeziumTimeDateToTimeFloat64(values.(float64))), op)
		} else {
			val = fmt.Sprintf("%[1]s %[3]s %.0[2]f", key, values.(float64), op)
		}
	case "string":
		val = fmt.Sprintf(`%[1]s %[3]s '%[2]s'`, key, values.(string), op)
	case "bool":
		val = values.(string)
	case "float", "double":
		val = fmt.Sprintf("%[1]s %[3]s %[2]f", key, values.(float64), op)
	}
	return val
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
		return t.MappingValueToCorrectType(c, t.ValAfter[c], operator.Equal)
	})
	result = append(result, fmt.Sprintf(INSERT_TABLE_TEMPLATE, fmt.Sprintf("%s.%s", t.Schema, t.TableName), strings.Join(col, ", "), strings.Join(vals, ", ")))
	return
}

func (t *TablePg) GenQueryUpdate() (result []string) {
	col := lo.Keys(t.ColWithKafka)
	var whereCon []string
	vals := lo.Map(col, func(c string, _ int) string {
		val := t.MappingValueToCorrectType(c, t.ValAfter[c], operator.Equal)
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

	if _, ok := t.ValBefore[constants.UpdatedAtField]; !ok {
		logger.Errorf("updated_at not existed, pls change mode or check gain")
		return []string{}
	}

	if _, ok := t.ValBefore[constants.DeletedAtField]; !ok {
		logger.Errorf("updated_at not existed, pls change mode or check gain")
		return []string{}
	}

	var whereCon []string
	vals := lo.Map(col, func(c string, _ int) string {
		val := t.MappingValueToCorrectType(c, t.ValAfter[c], operator.Equal)
		if lo.Contains(t.PrimaryKey, c) {
			whereCon = append(whereCon, val)
			return ""
		}
		if c == constants.UpdatedAtField {
			whereCon = append(whereCon, strings.ReplaceAll(val, "=", "<="))
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

func (t *TablePg) GenQueryDeleteWithKeys() (result []string) {
	var whereCon []string
	for k, v := range t.ValPrimaryKey {
		val := t.MappingValueToCorrectType(k, v, operator.Equal)
		whereCon = append(whereCon, val)
	}
	result = append(result, fmt.Sprintf(DELETE_TABLE_TEMPLATE,
		fmt.Sprintf("%s.%s", t.Schema, t.TableName),
		strings.Join(whereCon, " AND ")))
	return
}

func (t *TablePg) GenQueryDeleteExactlySingleNode() (result []string) {
	col := lo.Keys(t.ColWithKafka)
	whereCon := lo.Map(col, func(c string, _ int) string {
		// check field nullable
		return t.MappingValueToCorrectType(c, t.ValBefore[c], operator.Equal)
	})

	result = append(result, fmt.Sprintf(DELETE_TABLE_TEMPLATE,
		fmt.Sprintf("%s.%s", t.Schema, t.TableName),
		strings.Join(whereCon, " AND ")))
	return
}

func (t *TablePg) GenQueryDeleteExactlyMultiNode() (result []string) {
	if _, ok := t.ValBefore[constants.UpdatedAtField]; !ok {
		logger.Errorf("updated_at not existed, pls change mode or check gain")
		return []string{}
	}

	if _, ok := t.ValBefore[constants.DeletedAtField]; !ok {
		logger.Errorf("updated_at not existed, pls change mode or check gain")
		return []string{}
	}

	var whereCon []string
	for k, _ := range t.ValPrimaryKey {
		val := t.MappingValueToCorrectType(k, t.ValPrimaryKey[k], operator.Equal)
		whereCon = append(whereCon, val)
	}

	updatedVal := t.ValBefore[constants.UpdatedAtField]

	// case record didn't update yet.
	if updatedVal == nil {
		// pass
	} else {
		whereCon = append(whereCon, t.MappingValueToCorrectType(constants.UpdatedAtField, updatedVal, operator.LessThanEqual))
	}

	whereCon = append(whereCon, "deleted_at = false")

	result = append(result, fmt.Sprintf(DELETE_TABLE_TEMPLATE,
		fmt.Sprintf("%s.%s", t.Schema, t.TableName),
		strings.Join(whereCon, " AND ")))
	return
}
