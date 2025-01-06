package utils

import "testing"

func TestConvert(t *testing.T) {
	ConvertDebeziumTimeDateToTime(6020)
	ConvertDebeziumTimeDateToTime(-5965)
}
