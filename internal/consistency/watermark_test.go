package consistency

import (
	"testing"
	"time"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/extension"
)

func TestComputeWatermarkUsesEarlierClockAndSlowerSide(t *testing.T) {
	pubNow := time.Date(2026, 3, 3, 12, 0, 1, 0, time.UTC)
	subNow := time.Date(2026, 3, 3, 12, 0, 0, 0, time.UTC)

	window := ComputeWatermark(
		SideSnapshot{CapturedAt: pubNow, NaptimeMS: 100, DirtyQueueCount: 2},
		SideSnapshot{CapturedAt: subNow, NaptimeMS: 250, DirtyQueueCount: 3},
		500*time.Millisecond,
	)

	if !window.SharedAnchorAt.Equal(subNow) {
		t.Fatalf("expected earlier clock as anchor, got %s", window.SharedAnchorAt)
	}
	if window.EffectiveDelay != 750*time.Millisecond {
		t.Fatalf("expected 750ms delay, got %s", window.EffectiveDelay)
	}
	expectedCutoff := subNow.Add(-750 * time.Millisecond)
	if !window.SharedCutoffAt.Equal(expectedCutoff) {
		t.Fatalf("expected cutoff %s, got %s", expectedCutoff, window.SharedCutoffAt)
	}
}

func TestEqualEligibleBuckets(t *testing.T) {
	hashA := "a"
	hashB := "b"

	left := []extension.BucketHash{{
		SchemaName: "public",
		TableName:  "items",
		BucketID:   1,
		RowCount:   10,
		BucketHash: &hashA,
	}}
	right := []extension.BucketHash{{
		SchemaName: "public",
		TableName:  "items",
		BucketID:   1,
		RowCount:   10,
		BucketHash: &hashA,
	}}
	if !EqualEligibleBuckets(left, right) {
		t.Fatal("expected equal bucket sets")
	}

	right[0].BucketHash = &hashB
	if EqualEligibleBuckets(left, right) {
		t.Fatal("expected bucket sets to differ")
	}
}
