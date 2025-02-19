package adapter

import (
	"context"
	"data-replication/internal/logger"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgPoolAdapter struct {
	pool *pgxpool.Pool
}

func NewPgPoolAdapter(pool *pgxpool.Pool) *PgPoolAdapter {
	return &PgPoolAdapter{pool: pool}
}

func (p *PgPoolAdapter) GetAllTableNames(schema string) ([]string, error) {
	query := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE table_schema='%s' AND table_type='BASE TABLE'`, schema)
	fmt.Println(query)
	rows, err := p.pool.Query(context.Background(), query)
	if err != nil {
		logger.Errorf("GetAllTableNames query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	var tableName string
	var tableNames []string
	for rows.Next() {
		err := rows.Scan(&tableName)
		if err != nil {
			logger.Error("%v", err)
			continue
		}
		tableNames = append(tableNames, tableName)
	}
	return tableNames, nil
}

func (p *PgPoolAdapter) Close() {
	p.pool.Close()
}
