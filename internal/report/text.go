package report

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/compare"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/repairplan"
)

func WriteText(w io.Writer, publisherName, subscriberName string, summary compare.Summary) error {
	if _, err := fmt.Fprintf(
		w,
		"Compared %d/%d bucket(s) between %s and %s.\n",
		summary.ComparedBuckets,
		summary.TotalBuckets,
		publisherName,
		subscriberName,
	); err != nil {
		return err
	}

	if summary.ConsistencyMode != "" && summary.ConsistencyMode != "raw" {
		if _, err := fmt.Fprintf(
			w,
			"Consistency mode=%s snapshot_status=%s skipped=%d cutoff=%s retries=%d pub_dirty=%d sub_dirty=%d.\n",
			summary.ConsistencyMode,
			summary.SnapshotStatus,
			summary.SkippedBuckets,
			formatTime(summary.SharedCutoffAt),
			summary.StabilizationRetriesUsed,
			summary.PublisherDirtyQueueCount,
			summary.SubscriberDirtyQueueCount,
		); err != nil {
			return err
		}
	}

	if summary.SnapshotStatus == "unstable_snapshot" {
		if _, err := fmt.Fprintln(w, "Snapshot did not stabilize during verification; results may be incomplete."); err != nil {
			return err
		}
	}

	if summary.MismatchedBuckets == 0 {
		if summary.SkippedBuckets > 0 {
			_, err := fmt.Fprintln(w, "No mismatched buckets found among the compared stable buckets.")
			return err
		}
		_, err := fmt.Fprintln(w, "No mismatched buckets found.")
		return err
	}

	if _, err := fmt.Fprintf(w, "Found %d mismatched bucket(s):\n", summary.MismatchedBuckets); err != nil {
		return err
	}
	for _, diff := range summary.Diffs {
		if _, err := fmt.Fprintf(
			w,
			"- %s.%s bucket=%d pk=[%s,%s) status=%s pub_count=%s sub_count=%s\n",
			diff.SchemaName,
			diff.TableName,
			diff.BucketID,
			formatInt(diff.PKStart),
			formatInt(diff.PKEnd),
			diff.Status,
			formatInt(diff.PublisherCount),
			formatInt(diff.SubscriberCount),
		); err != nil {
			return err
		}
	}
	return nil
}

func WriteJSON(w io.Writer, publisherName, subscriberName string, summary compare.Summary) error {
	payload := struct {
		Publisher  string          `json:"publisher"`
		Subscriber string          `json:"subscriber"`
		Summary    compare.Summary `json:"summary"`
	}{
		Publisher:  publisherName,
		Subscriber: subscriberName,
		Summary:    summary,
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(payload)
}

func WriteInspectText(w io.Writer, publisherName, subscriberName string, summary compare.InspectSummary) error {
	if _, err := fmt.Fprintf(
		w,
		"Inspected %s.%s bucket=%d pk_column=%s range=[%d,%d) between %s and %s.\n",
		summary.SchemaName,
		summary.TableName,
		summary.BucketID,
		summary.PKColumn,
		summary.PKStart,
		summary.PKEnd,
		publisherName,
		subscriberName,
	); err != nil {
		return err
	}

	if summary.MismatchedRows == 0 {
		_, err := fmt.Fprintln(w, "No mismatched rows found in this bucket.")
		return err
	}

	if _, err := fmt.Fprintf(w, "Found %d mismatched row(s):\n", summary.MismatchedRows); err != nil {
		return err
	}
	for _, diff := range summary.Diffs {
		if _, err := fmt.Fprintf(w, "- pk=%s status=%s\n", diff.PKValue, diff.Status); err != nil {
			return err
		}
		if diff.RepairSQL != "" {
			if _, err := fmt.Fprintf(w, "  repair_sql: %s\n", diff.RepairSQL); err != nil {
				return err
			}
		}
	}

	return nil
}

func WriteInspectJSON(w io.Writer, publisherName, subscriberName string, summary compare.InspectSummary) error {
	payload := struct {
		Publisher  string                 `json:"publisher"`
		Subscriber string                 `json:"subscriber"`
		Summary    compare.InspectSummary `json:"summary"`
	}{
		Publisher:  publisherName,
		Subscriber: subscriberName,
		Summary:    summary,
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(payload)
}

func WriteRepairText(w io.Writer, publisherName, subscriberName string, summary repairplan.ApplySummary) error {
	if _, err := fmt.Fprintf(
		w,
		"Applied %d/%d repair statement(s) to %s for %s.%s bucket=%d using %s as source.\n",
		summary.StatementsApplied,
		summary.StatementsPlanned,
		subscriberName,
		summary.SchemaName,
		summary.TableName,
		summary.BucketID,
		publisherName,
	); err != nil {
		return err
	}

	if summary.StatementsPlanned == 0 {
		_, err := fmt.Fprintln(w, "No repair actions were needed.")
		return err
	}

	return nil
}

func WriteRepairJSON(w io.Writer, publisherName, subscriberName string, summary repairplan.ApplySummary) error {
	payload := struct {
		Publisher  string                  `json:"publisher"`
		Subscriber string                  `json:"subscriber"`
		Summary    repairplan.ApplySummary `json:"summary"`
	}{
		Publisher:  publisherName,
		Subscriber: subscriberName,
		Summary:    summary,
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(payload)
}

func formatInt(value *int64) string {
	if value == nil {
		return "null"
	}
	return fmt.Sprintf("%d", *value)
}

func formatTime(value *time.Time) string {
	if value == nil {
		return "null"
	}
	return value.Format(time.RFC3339Nano)
}
