package repairplan

import (
	"context"
	"fmt"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/compare"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ApplySummary struct {
	SchemaName        string                 `json:"schema_name"`
	TableName         string                 `json:"table_name"`
	BucketID          int64                  `json:"bucket_id"`
	StatementsPlanned int                    `json:"statements_planned"`
	StatementsApplied int                    `json:"statements_applied"`
	InspectSummary    compare.InspectSummary `json:"inspect_summary"`
}

func ExecuteInspectPlans(ctx context.Context, pool *pgxpool.Pool, summary compare.InspectSummary) (ApplySummary, error) {
	result := ApplySummary{
		SchemaName:        summary.SchemaName,
		TableName:         summary.TableName,
		BucketID:          summary.BucketID,
		InspectSummary:    summary,
		StatementsPlanned: countStatements(summary),
	}

	if result.StatementsPlanned == 0 {
		return result, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return ApplySummary{}, fmt.Errorf("begin repair transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, diff := range summary.Diffs {
		if diff.RepairSQL == "" {
			continue
		}
		if _, err := tx.Exec(ctx, diff.RepairSQL); err != nil {
			return ApplySummary{}, fmt.Errorf("execute repair for pk=%s: %w", diff.PKValue, err)
		}
		result.StatementsApplied++
	}

	if err := tx.Commit(ctx); err != nil {
		return ApplySummary{}, fmt.Errorf("commit repair transaction: %w", err)
	}

	return result, nil
}

func countStatements(summary compare.InspectSummary) int {
	total := 0
	for _, diff := range summary.Diffs {
		if diff.RepairSQL != "" {
			total++
		}
	}
	return total
}
