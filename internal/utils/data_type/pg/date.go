package utils

import (
	"time"
)

// Type in postgres is Date
// https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-data-types
/*
DATE   |  INT32 |  io.debezium.time.Date  Represents the number of days since the epoch.

Example:
In DB:  1953-09-02 => Message: -5965
        1986-06-26              6020
*/

/*
 Base time on 1 January 1970
*/

func ConvertDebeziumTimeDateToTime(epochDate int32) time.Time {
	return time.Unix(0, 0).Add(time.Duration(epochDate) * 24 * time.Hour)
}
