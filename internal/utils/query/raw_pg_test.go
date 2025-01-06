package query

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestRawPgInsertQuery(t *testing.T) {
	Convey("Testing Insert query", t, func() {
		mockDataMap := map[string]interface{}{
			"id":          "d010",
			"dept_name":   "Test Debezium",
			"number_test": 2,
		}
		result := GenInsertQueryFromMap("testTable", mockDataMap)

		fmt.Println(result)
	})
}
