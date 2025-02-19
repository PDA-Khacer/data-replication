package application

import (
	"data-replication/config"
	"data-replication/internal/infrastructure"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestIntegrationDataReplication(t *testing.T) {
	infrastructure.Cfg = &config.Config{
		ServiceName: "",
		Cdc: config.Cdc{
			Debezium: config.Debezium{
				Type:  "",
				Host:  "",
				Port:  0,
				Kafka: config.Kafka{},
				MappingSourceMap: []*config.MappingSource{
					{
						Name:                    "test",
						CheckCreate:             false,
						Schema:                  "",
						Prefix:                  "",
						Mode:                    "",
						IsFetchingSourceDbTable: true,
						SourceDbTable:           nil,
						CombineConcurrency: &config.CombineConcurrency{
							Status:  false,
							Combine: nil,
						},
						Topic:                nil,
						SkipOperator:         nil,
						SourceDb:             "",
						DestinationDb:        "",
						DestinationDbCombine: nil,
					},
				},
			},
		},
		SourceDbMap:      nil,
		DestinationDbMap: nil,
	}
	Convey("Testing real", t, func() {
		for _, mapSource := range infrastructure.Cfg.Cdc.Debezium.MappingSourceMap {
			if mapSource.IsFetchingSourceDbTable == true {
				// fetching table name
				fmt.Println(mapSource.Name)
				infrastructure.SetSourceDBOfCdc(mapSource.Name, []string{"ssss"})
			}

			// each topic run singe
			if mapSource.CombineConcurrency.Status == false {
				for _, v := range mapSource.SourceDbTable {
					fmt.Println(v)
				}
			}
		}

	})
}
