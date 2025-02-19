package adapter

import (
	"context"
	"data-replication/config"
	"data-replication/internal/logger"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestIntegrationTestPgPool(t *testing.T) {
	Convey("Testing real pg", t, func() {
		ctx := context.Background()
		dbconfig := config.DbConfig{
			Alias:          "pgDesDb",
			Type:           "postgres",
			Host:           "localhost",
			Port:           5432,
			Username:       "postgres",
			Password:       "postgres",
			Db:             "employees",
			PoolMaxConnect: 1,
			PoolMinConnect: 1,
		}

		psqlInfo := fmt.Sprintf(`postgresql://%[1]s:%[2]d/%[7]s?user=%[3]s&password=%[4]s&pool_max_conns=%[5]d&pool_min_conns=%[6]d&target_session_attrs=read-write`,
			dbconfig.Host, dbconfig.Port, dbconfig.Username, dbconfig.Password, dbconfig.PoolMaxConnect, dbconfig.PoolMinConnect, dbconfig.Db)

		pool := &pgxpool.Pool{}
		fmt.Println(psqlInfo)
		pool, err := pgxpool.New(ctx, psqlInfo)
		if err != nil {
			panic(err)
		}

		err = pool.Ping(ctx)
		if err != nil {
			logger.Errorf("ping postgres e"+
				""+
				"rror: %v", err)
			panic(err)
		}

		p := NewPgPoolAdapter(pool)

		names, err := p.GetAllTableNames("employees")
		if err != nil {
			panic(err)
		}
		for _, name := range names {
			fmt.Println(name)
		}
	})
}
