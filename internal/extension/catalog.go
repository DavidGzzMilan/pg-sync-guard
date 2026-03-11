package extension

import (
	"context"
	"encoding/json"
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

type MonitoredTable struct {
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	PKColumn   string `json:"pk_column"`
	BucketSize int64  `json:"bucket_size"`
}

type TableColumn struct {
	Name             string `json:"name"`
	TypeSQL          string `json:"type_sql"`
	IsGenerated      bool   `json:"is_generated"`
	IsIdentityAlways bool   `json:"is_identity_always"`
}

type BucketRow struct {
	PKValue string          `json:"pk_value"`
	RowData json.RawMessage `json:"row_data"`
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

func FetchMonitoredTable(ctx context.Context, pool *pgxpool.Pool, schema, table string) (MonitoredTable, error) {
	const sql = `
SELECT
    schema_name,
    table_name,
    pk_column,
    COALESCE(bucket_size, current_setting('syncguard.chunk_size', true)::bigint)
FROM syncguard.monitored_tables
WHERE schema_name = $1
  AND table_name = $2`

	var meta MonitoredTable
	err := pool.QueryRow(ctx, sql, strings.TrimSpace(schema), strings.TrimSpace(table)).Scan(
		&meta.SchemaName,
		&meta.TableName,
		&meta.PKColumn,
		&meta.BucketSize,
	)
	if err != nil {
		return MonitoredTable{}, fmt.Errorf("query monitored table metadata: %w", err)
	}
	return meta, nil
}

func FetchBucketRows(ctx context.Context, pool *pgxpool.Pool, meta MonitoredTable, bucketID int64) ([]BucketRow, int64, int64, error) {
	pkStart := bucketID * meta.BucketSize
	pkEnd := pkStart + meta.BucketSize

	sql := fmt.Sprintf(`
SELECT
    t.%s::text AS pk_value,
    row_to_json(t)::jsonb AS row_data
FROM %s AS t
WHERE t.%s >= $1
  AND t.%s < $2
ORDER BY t.%s`,
		quoteIdentifier(meta.PKColumn),
		quoteQualifiedIdentifier(meta.SchemaName, meta.TableName),
		quoteIdentifier(meta.PKColumn),
		quoteIdentifier(meta.PKColumn),
		quoteIdentifier(meta.PKColumn),
	)

	rows, err := pool.Query(ctx, sql, pkStart, pkEnd)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("query bucket rows: %w", err)
	}
	defer rows.Close()

	var bucketRows []BucketRow
	for rows.Next() {
		var row BucketRow
		if err := rows.Scan(&row.PKValue, &row.RowData); err != nil {
			return nil, 0, 0, fmt.Errorf("scan bucket row: %w", err)
		}
		bucketRows = append(bucketRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, fmt.Errorf("iterate bucket rows: %w", err)
	}

	return bucketRows, pkStart, pkEnd, nil
}

func FetchTableColumns(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]TableColumn, error) {
	const sql = `
SELECT
    a.attname,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_sql,
    a.attgenerated = 's' AS is_generated,
    a.attidentity = 'a' AS is_identity_always
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c
  ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n
  ON n.oid = c.relnamespace
WHERE n.nspname = $1
  AND c.relname = $2
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum`

	rows, err := pool.Query(ctx, sql, strings.TrimSpace(schema), strings.TrimSpace(table))
	if err != nil {
		return nil, fmt.Errorf("query table columns: %w", err)
	}
	defer rows.Close()

	var columns []TableColumn
	for rows.Next() {
		var column TableColumn
		if err := rows.Scan(&column.Name, &column.TypeSQL, &column.IsGenerated, &column.IsIdentityAlways); err != nil {
			return nil, fmt.Errorf("scan table column: %w", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table columns: %w", err)
	}

	return columns, nil
}

func WorkerRunning(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var running bool
	if err := pool.QueryRow(ctx, "SELECT syncguard_worker_running()").Scan(&running); err != nil {
		return false, fmt.Errorf("query worker status: %w", err)
	}
	return running, nil
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func quoteQualifiedIdentifier(schemaName, tableName string) string {
	return quoteIdentifier(schemaName) + "." + quoteIdentifier(tableName)
}
