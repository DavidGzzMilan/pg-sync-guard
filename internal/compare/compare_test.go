package compare

import (
	"testing"
	"time"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/extension"
)

func TestCompareBucketsDetectsHashMismatch(t *testing.T) {
	pubHash := "pub"
	subHash := "sub"

	summary := CompareBuckets(
		[]extension.BucketHash{{
			SchemaName: "public",
			TableName:  "items",
			BucketID:   4,
			PKStart:    400,
			PKEnd:      500,
			RowCount:   10,
			BucketHash: &pubHash,
		}},
		[]extension.BucketHash{{
			SchemaName: "public",
			TableName:  "items",
			BucketID:   4,
			PKStart:    400,
			PKEnd:      500,
			RowCount:   10,
			BucketHash: &subHash,
		}},
	)

	if summary.TotalBuckets != 1 {
		t.Fatalf("expected total buckets 1, got %d", summary.TotalBuckets)
	}
	if summary.MismatchedBuckets != 1 {
		t.Fatalf("expected mismatched buckets 1, got %d", summary.MismatchedBuckets)
	}
	if len(summary.Diffs) != 1 {
		t.Fatalf("expected one diff, got %d", len(summary.Diffs))
	}
	if summary.Diffs[0].Status != "hash_mismatch" {
		t.Fatalf("expected hash_mismatch, got %s", summary.Diffs[0].Status)
	}
}

func TestInspectBucketDetectsMissingAndChangedRows(t *testing.T) {
	meta := extension.MonitoredTable{
		SchemaName: "public",
		TableName:  "items",
		PKColumn:   "id",
		BucketSize: 100,
	}

	summary := InspectBucket(
		meta,
		4,
		400,
		500,
		[]extension.BucketRow{
			{PKValue: "401", RowData: []byte(`{"id":401,"payload":"a"}`)},
			{PKValue: "402", RowData: []byte(`{"id":402,"payload":"b"}`)},
		},
		[]extension.BucketRow{
			{PKValue: "401", RowData: []byte(`{"id":401,"payload":"changed"}`)},
			{PKValue: "403", RowData: []byte(`{"id":403,"payload":"c"}`)},
		},
	)

	if summary.MismatchedRows != 3 {
		t.Fatalf("expected 3 mismatched rows, got %d", summary.MismatchedRows)
	}
	if len(summary.Diffs) != 3 {
		t.Fatalf("expected 3 row diffs, got %d", len(summary.Diffs))
	}
	if summary.Diffs[0].Status != "row_mismatch" {
		t.Fatalf("expected first diff to be row_mismatch, got %s", summary.Diffs[0].Status)
	}
	if summary.Diffs[1].Status != "missing_on_subscriber" {
		t.Fatalf("expected second diff to be missing_on_subscriber, got %s", summary.Diffs[1].Status)
	}
	if summary.Diffs[2].Status != "missing_on_publisher" {
		t.Fatalf("expected third diff to be missing_on_publisher, got %s", summary.Diffs[2].Status)
	}
}

func TestCompareStableBucketsSkipsUnstableBuckets(t *testing.T) {
	pubHashA := "a"
	subHashA := "a"
	pubHashB := "b1"
	subHashB := "b2"
	cutoff := time.Date(2026, 3, 3, 12, 0, 0, 0, time.UTC)
	captured := cutoff.Add(500 * time.Millisecond)

	publisherAll := []extension.BucketHash{
		{SchemaName: "public", TableName: "items", BucketID: 1, PKStart: 0, PKEnd: 100, RowCount: 10, BucketHash: &pubHashA},
		{SchemaName: "public", TableName: "items", BucketID: 2, PKStart: 100, PKEnd: 200, RowCount: 5, BucketHash: &pubHashB},
	}
	subscriberAll := []extension.BucketHash{
		{SchemaName: "public", TableName: "items", BucketID: 1, PKStart: 0, PKEnd: 100, RowCount: 10, BucketHash: &subHashA},
		{SchemaName: "public", TableName: "items", BucketID: 2, PKStart: 100, PKEnd: 200, RowCount: 5, BucketHash: &subHashB},
	}

	summary := CompareStableBuckets(
		publisherAll,
		subscriberAll,
		[]extension.BucketHash{publisherAll[0]},
		[]extension.BucketHash{subscriberAll[0]},
		StableCompareMetadata{
			ConsistencyMode:           "stable-watermark",
			SnapshotStatus:            "stable",
			PublisherCapturedAt:       &captured,
			SubscriberCapturedAt:      &captured,
			SharedCutoffAt:            &cutoff,
			StabilizationRetriesUsed:  1,
			PublisherDirtyQueueCount:  1,
			SubscriberDirtyQueueCount: 2,
		},
	)

	if summary.TotalBuckets != 2 {
		t.Fatalf("expected total buckets 2, got %d", summary.TotalBuckets)
	}
	if summary.ComparedBuckets != 1 {
		t.Fatalf("expected compared buckets 1, got %d", summary.ComparedBuckets)
	}
	if summary.SkippedBuckets != 1 {
		t.Fatalf("expected skipped buckets 1, got %d", summary.SkippedBuckets)
	}
	if summary.MismatchedBuckets != 0 {
		t.Fatalf("expected mismatched buckets 0, got %d", summary.MismatchedBuckets)
	}
	if summary.SnapshotStatus != "stable" {
		t.Fatalf("expected stable snapshot status, got %s", summary.SnapshotStatus)
	}
}
