package config

import (
	"flag"
	"fmt"
	"log"

	"os"

	"github.com/go-playground/validator/v10"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var configPath string
var Cfg Config

type Config struct {
	ServiceName      string     `mapstructure:"serviceName" validate:"required"`
	Cdc              Cdc        `mapstructure:"cdc" validate:"required"`
	SourceDbMap      []DbConfig `mapstructure:"sourceDb" validate:"required"`
	DestinationDbMap []DbConfig `mapstructure:"destinationDb" validate:"required"`
}

// Cdc Capture data change config
type Cdc struct {
	Debezium Debezium `mapstructure:"debezium" validate:"required"`
}

// Debezium Capture data change framework
type Debezium struct {
	Type             string          `mapstructure:"type" validate:"required"`
	Host             string          `mapstructure:"host" validate:"required"`
	Port             int             `mapstructure:"port" validate:"required"`
	Kafka            Kafka           `mapstructure:"kafka" validate:"required"`
	MappingSourceMap []MappingSource `mapstructure:"mappingSource" validate:"required"`
}

// Kafka which debezium send data to
type Kafka struct {
	Servers    string `mapstructure:"servers" validate:"required"`
	AutoCommit bool   `mapstructure:"autoCommit"`
}

// MappingSource detail of connector
type MappingSource struct {
	Name          string   `mapstructure:"name" validate:"required"`
	CheckCreate   bool     `mapstructure:"checkCreate"`
	Schema        string   `mapstructure:"schema" validate:"required"`
	Prefix        string   `mapstructure:"prefix" validate:"required"`
	Mode          string   `mapstructure:"mode" validate:"required"`
	SourceDbTable []string `mapstructure:"sourceDbTable" validate:"required"`
	// CombineConcurrency null = false
	CombineConcurrency   *CombineConcurrency   `mapstructure:"combineConcurrency"`
	Topic                *[]string             `mapstructure:"topic"`
	SkipOperator         *[]string             `mapstructure:"skipOperator"`
	SourceDb             string                `mapstructure:"sourceDb" validate:"required"`
	DestinationDb        string                `mapstructure:"destinationDb" validate:"required"`
	DestinationDbCombine *DestinationDbCombine `mapstructure:"destinationDbCombine" validate:"required"`
}

// CombineConcurrency support handler many topic same concurrency
type CombineConcurrency struct {
	Status  bool     `mapstructure:"status"`
	Combine []string `mapstructure:"combine" validate:"required"`
}

// DestinationDbCombine define combine table out put like: have table A,B => to only C
type DestinationDbCombine struct {
	Combine       bool         `mapstructure:"combine"`
	CombineMapMap []CombineMap `mapstructure:"combineMap" validate:"required"`
}

// CombineMap define way to combine and primary key will combine
type CombineMap struct {
	CombineTableSrc      []string `mapstructure:"combineTableSrc" validate:"required"`
	CombineDesTable      string   `mapstructure:"combineDesTable" validate:"required"`
	CombineDesPrimaryKey string   `mapstructure:"combineDesPrimaryKey" validate:"required"`
}

// DbConfig config of database
type DbConfig struct {
	Alias          string `mapstructure:"alias" validate:"required"`
	Type           string `mapstructure:"type" validate:"required"`
	Host           string `mapstructure:"host" validate:"required"`
	Port           int    `mapstructure:"port" validate:"required"`
	Username       string `mapstructure:"username" validate:"required"`
	Password       string `mapstructure:"password" validate:"required"`
	Db             string `mapstructure:"db" validate:"required"`
	PoolMaxConnect int    `mapstructure:"poolMaxConnect"`
	PoolMinConnect int    `mapstructure:"poolMinConnect"`
}

func init() {
	flag.StringVar(&configPath, "config", "", "ES microservice config path")
}

func SetConfigPath(path string) {
	configPath = path
}

func InitConfig() (*Config, error) {
	if configPath == "" {
		configPathFromEnv := os.Getenv("config/config.yaml")
		if configPathFromEnv != "" {
			configPath = configPathFromEnv
		} else {
			getwd, err := os.Getwd()
			if err != nil {
				return nil, errors.Wrap(err, "os.Getwd")
			}
			configPath = fmt.Sprintf("%s/config/config.yaml", getwd)
		}
	}

	var cfg Config

	viper.SetConfigType("yaml")
	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "viper.ReadInConfig")
	}

	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, errors.Wrap(err, "viper.Unmarshal")
	}

	validate := validator.New(validator.WithRequiredStructEnabled())
	if err := validate.Struct(&cfg); err != nil {
		log.Fatalf("Missing required attributes %v\n", err)
	}

	Cfg = cfg

	return &cfg, nil
}
