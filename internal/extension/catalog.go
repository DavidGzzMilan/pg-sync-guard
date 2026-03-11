package extension

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type BucketHash struct {
	SchemaName     string     `json:"schema_name"`
	TableName      string     `json:"table_name"`
	BucketID       int64      `json:"bucket_id"`
	PKStart        int64      `json:"pk_start"`
	PKEnd          int64      `json:"pk_end"`
	RowCount       int64      `json:"row_count"`
	BucketHash     *string    `json:"bucket_hash"`
	Dirty          bool       `json:"dirty"`
	LastComputedAt time.Time  `json:"last_computed_at"`
	ReviewedAt     *time.Time `json:"reviewed_at,omitempty"`
}

type WorkerStatus struct {
	Running bool `json:"running"`
}

func CurrentDatabaseName(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	var dbName string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&dbName); err != nil {
		return "", fmt.Errorf("query current database: %w", err)
	}
	return dbName, nil
}

func FetchBucketCatalog(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]BucketHash, error) {
	const sql = `
SELECT
    schema_name,
    table_name,
    bucket_id,
    pk_start,
    pk_end,
    row_count,
    bucket_hash,
    dirty,
    last_computed_at,
    reviewed_at
FROM syncguard.bucket_catalog
WHERE ($1 = '' OR schema_name = $1)
  AND ($2 = '' OR table_name = $2)
ORDER BY schema_name, table_name, bucket_id`

	rows, err := pool.Query(ctx, sql, strings.TrimSpace(schema), strings.TrimSpace(table))
	if err != nil {
		return nil, fmt.Errorf("query bucket_catalog: %w", err)
	}
	defer rows.Close()

	var buckets []BucketHash
	for rows.Next() {
		var bucket BucketHash
		if err := rows.Scan(
			&bucket.SchemaName,
			&bucket.TableName,
			&bucket.BucketID,
			&bucket.PKStart,
			&bucket.PKEnd,
			&bucket.RowCount,
			&bucket.BucketHash,
			&bucket.Dirty,
			&bucket.LastComputedAt,
			&bucket.ReviewedAt,
		); err != nil {
			return nil, fmt.Errorf("scan bucket_catalog row: %w", err)
		}
		buckets = append(buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bucket_catalog rows: %w", err)
	}

	return buckets, nil
}

func WorkerRunning(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var running bool
	if err := pool.QueryRow(ctx, "SELECT syncguard_worker_running()").Scan(&running); err != nil {
		return false, fmt.Errorf("query worker status: %w", err)
	}
	return running, nil
}
