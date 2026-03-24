package consistency

import (
	"time"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/extension"
)

const (
	ModeRaw             = "raw"
	ModeStableWatermark = "stable-watermark"
)

type SideSnapshot struct {
	CapturedAt        time.Time  `json:"captured_at"`
	NaptimeMS         int        `json:"naptime_ms"`
	DirtyQueueCount   int        `json:"dirty_queue_count"`
	OldestDirtyQueued *time.Time `json:"oldest_dirty_queued_at,omitempty"`
}

type Watermark struct {
	PublisherCapturedAt  time.Time     `json:"publisher_captured_at"`
	SubscriberCapturedAt time.Time     `json:"subscriber_captured_at"`
	SharedAnchorAt       time.Time     `json:"shared_anchor_at"`
	SharedCutoffAt       time.Time     `json:"shared_cutoff_at"`
	EffectiveDelay       time.Duration `json:"effective_delay"`
	StabilityBuffer      time.Duration `json:"stability_buffer"`
	PublisherDirtyCount  int           `json:"publisher_dirty_count"`
	SubscriberDirtyCount int           `json:"subscriber_dirty_count"`
}

func ComputeWatermark(publisher, subscriber SideSnapshot, stabilityBuffer time.Duration) Watermark {
	anchor := publisher.CapturedAt
	if subscriber.CapturedAt.Before(anchor) {
		anchor = subscriber.CapturedAt
	}

	naptime := publisher.NaptimeMS
	if subscriber.NaptimeMS > naptime {
		naptime = subscriber.NaptimeMS
	}

	delay := time.Duration(naptime)*time.Millisecond + stabilityBuffer

	return Watermark{
		PublisherCapturedAt:  publisher.CapturedAt,
		SubscriberCapturedAt: subscriber.CapturedAt,
		SharedAnchorAt:       anchor,
		SharedCutoffAt:       anchor.Add(-delay),
		EffectiveDelay:       delay,
		StabilityBuffer:      stabilityBuffer,
		PublisherDirtyCount:  publisher.DirtyQueueCount,
		SubscriberDirtyCount: subscriber.DirtyQueueCount,
	}
}

func EqualEligibleBuckets(left, right []extension.BucketHash) bool {
	if len(left) != len(right) {
		return false
	}

	for i := range left {
		if left[i].SchemaName != right[i].SchemaName ||
			left[i].TableName != right[i].TableName ||
			left[i].BucketID != right[i].BucketID ||
			left[i].RowCount != right[i].RowCount ||
			deref(left[i].BucketHash) != deref(right[i].BucketHash) {
			return false
		}
	}

	return true
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
