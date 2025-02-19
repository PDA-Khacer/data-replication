package infrastructure

import (
	"data-replication/config"
	"github.com/samber/lo"
)

var Cfg *config.Config

func SetSourceDBOfCdc(mappingSourceKey string, tbs []string) {
	mapSource, found := lo.Find(Cfg.Cdc.Debezium.MappingSourceMap, func(item *config.MappingSource) bool {
		return item.Name == mappingSourceKey
	})
	if found {
		mapSource.SourceDbTable = tbs
	}
}
