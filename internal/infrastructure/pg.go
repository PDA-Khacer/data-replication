package infrastructure

import (
	"context"
	"data-replication/config"
	"data-replication/internal/logger"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/gorm"
)

var PgGorm *gorm.DB

var PgPool map[string]*pgxpool.Pool

func init() {
	PgPool = make(map[string]*pgxpool.Pool)
}

func AddOrReplacePgPool(key string, config config.DbConfig, ctx context.Context) error {
	psqlInfo := fmt.Sprintf(`postgresql://%[1]s:%[2]d?user=%[3]s&password=%[4]s&pool_max_conns=%[5]d&pool_min_conns=%[6]d&target_session_attrs=read-write`,
		config.Host, config.Port, config.Username, config.Password, config.PoolMaxConnect, config.PoolMinConnect)

	pool := &pgxpool.Pool{}

	pool, err := pgxpool.New(ctx, psqlInfo)
	if err != nil {
		return err
	}

	err = pool.Ping(ctx)
	if err != nil {
		logger.Errorf("ping postgres error: %v", err)
		return err
	}

	PgPool[key] = pool
	return nil
}

func GetPgPool(key string) *pgxpool.Pool {
	return PgPool[key]
}

func RemovePgPool(key string) error {
	if conn, ok := PgPool[key]; ok {
		conn.Close()
		delete(PgPool, key)
	}
	return nil
}
