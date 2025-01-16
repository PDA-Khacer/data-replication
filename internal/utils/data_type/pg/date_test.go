package utils

import (
	"fmt"
	"testing"
)

func TestConvert(t *testing.T) {
	c1 := ConvertDebeziumTimeDateToTime(-2039)
	c2 := ConvertDebeziumTimeDateToTime(5803)

	fmt.Println(GetDateOnlyYMD(c1))
	fmt.Println(GetDateOnlyYMDTime(c2))
	fmt.Println(c2.Format("15:04:05"))
}
