package utils

import (
	"fmt"
	"strconv"
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
	return time.Unix(0, 0).Add(time.Duration(epochDate) * 24 * time.Hour).UTC()
}

func ConvertDebeziumTimeDateToTimeFloat64(epochDate float64) time.Time {
	temp := fmt.Sprintf("%.0f", epochDate)
	i, _ := strconv.Atoi(temp)
	return time.Unix(0, 0).Add(time.Duration(i) * 24 * time.Hour).UTC()
}

func ConvertDebeziumMicroTimestampToTime(t float64) time.Time {
	return time.UnixMicro(int64(t)).UTC()
}

func ConvertDebeziumMilliTimestampToTime(t float64) time.Time {
	return time.UnixMilli(int64(t)).UTC()
}

func ConvertDebeziumMircoTimeToTime(t float64) time.Time {
	return time.Unix(0, 0).Add(time.Duration(t) * time.Microsecond).UTC()
}

func ConvertDebeziumTimeToTime(t float64) time.Time {
	return time.Unix(0, 0).Add(time.Duration(t) * time.Millisecond).UTC()
}

func GetDateOnlyYMD(t time.Time) string {
	var m string
	var d string
	if t.Month() < 10 {
		m = "0" + strconv.Itoa(int(t.Month()))
	} else {
		m = strconv.Itoa(int(t.Month()))
	}
	if t.Day() < 10 {
		d = "0" + strconv.Itoa(t.Day())
	} else {
		d = strconv.Itoa(t.Day())
	}

	return fmt.Sprintf("%d-%s-%s",
		t.Year(),
		m,
		d)
}

func GetDateOnlyYMDTime(t time.Time) string {
	var m string
	var d string
	var h string
	var mm string
	var s string
	var ns string

	if t.Month() < 10 {
		m = "0" + strconv.Itoa(int(t.Month()))
	} else {
		m = strconv.Itoa(int(t.Month()))
	}
	if t.Day() < 10 {
		d = "0" + strconv.Itoa(t.Day())
	} else {
		d = strconv.Itoa(t.Day())
	}
	if t.Hour() < 10 {
		h = "0" + strconv.Itoa(t.Hour())
	} else {
		h = strconv.Itoa(t.Hour())
	}
	if t.Minute() < 10 {
		mm = "0" + strconv.Itoa(t.Minute())
	} else {
		mm = strconv.Itoa(t.Minute())
	}
	if t.Second() < 10 {
		s = "0" + strconv.Itoa(t.Second())
	} else {
		s = strconv.Itoa(t.Second())
	}
	nsStr := strconv.Itoa(t.Nanosecond())
	for i := 0; i < 6-len(nsStr); i++ {
		ns += "0"
	}
	ns += nsStr
	return fmt.Sprintf("%d-%s-%s %s:%s:%s.%s",
		t.Year(),
		m,
		d,
		h,
		mm,
		s,
		ns)
}
