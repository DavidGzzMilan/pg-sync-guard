package controlplane

import (
	"context"
	"fmt"
	"strings"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/compare"
	"github.com/jackc/pgx/v5/pgxpool"
)

func WriteVerifyRun(
	ctx context.Context,
	pool *pgxpool.Pool,
	publisherName string,
	subscriberName string,
	tableFilter string,
	summary compare.Summary,
) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin control plane transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var runID string
	err = tx.QueryRow(ctx, `
INSERT INTO syncguard.validation_runs (
    publisher_name,
    subscriber_name,
    table_filter,
    status
)
VALUES ($1, $2, NULLIF($3, ''), 'running')
RETURNING run_id`,
		publisherName,
		subscriberName,
		strings.TrimSpace(tableFilter),
	).Scan(&runID)
	if err != nil {
		return fmt.Errorf("insert validation run: %w", err)
	}

	for _, diff := range summary.Diffs {
		_, err = tx.Exec(ctx, `
INSERT INTO syncguard.divergence_log (
    run_id,
    schema_name,
    table_name,
    bucket_id,
    pk_start,
    pk_end,
    publisher_row_count,
    subscriber_row_count,
    publisher_hash,
    subscriber_hash,
    status
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'open')`,
			runID,
			diff.SchemaName,
			diff.TableName,
			diff.BucketID,
			valueOrZero(diff.PKStart),
			valueOrZero(diff.PKEnd),
			valueOrNil(diff.PublisherCount),
			valueOrNil(diff.SubscriberCount),
			valueOrNil(diff.PublisherHash),
			valueOrNil(diff.SubscriberHash),
		)
		if err != nil {
			return fmt.Errorf("insert divergence log: %w", err)
		}
	}

	status := "success"
	if summary.MismatchedBuckets > 0 {
		status = "diverged"
	}

	_, err = tx.Exec(ctx, `
UPDATE syncguard.validation_runs
SET total_buckets_compared = $2,
    mismatched_buckets = $3,
    status = $4,
    finished_at = now()
WHERE run_id = $1`,
		runID,
		summary.TotalBuckets,
		summary.MismatchedBuckets,
		status,
	)
	if err != nil {
		return fmt.Errorf("finalize validation run: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit control plane transaction: %w", err)
	}
	return nil
}

func valueOrZero(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func valueOrNil[T any](value *T) *T {
	return value
}
