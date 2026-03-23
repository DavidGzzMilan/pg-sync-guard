package compare

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/extension"
)

type BucketKey struct {
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	BucketID   int64  `json:"bucket_id"`
}

type BucketDiff struct {
	SchemaName      string  `json:"schema_name"`
	TableName       string  `json:"table_name"`
	BucketID        int64   `json:"bucket_id"`
	PKStart         *int64  `json:"pk_start,omitempty"`
	PKEnd           *int64  `json:"pk_end,omitempty"`
	PublisherHash   *string `json:"publisher_hash,omitempty"`
	SubscriberHash  *string `json:"subscriber_hash,omitempty"`
	PublisherCount  *int64  `json:"publisher_row_count,omitempty"`
	SubscriberCount *int64  `json:"subscriber_row_count,omitempty"`
	Status          string  `json:"status"`
}

type Summary struct {
	TotalBuckets              int          `json:"total_buckets"`
	ComparedBuckets           int          `json:"compared_buckets"`
	SkippedBuckets            int          `json:"skipped_buckets"`
	SkippedOnPublisher        int          `json:"skipped_on_publisher"`
	SkippedOnSubscriber       int          `json:"skipped_on_subscriber"`
	MismatchedBuckets         int          `json:"mismatched_buckets"`
	Diffs                     []BucketDiff `json:"diffs"`
	ConsistencyMode           string       `json:"consistency_mode,omitempty"`
	SnapshotStatus            string       `json:"snapshot_status,omitempty"`
	PublisherCapturedAt       *time.Time   `json:"publisher_captured_at,omitempty"`
	SubscriberCapturedAt      *time.Time   `json:"subscriber_captured_at,omitempty"`
	SharedCutoffAt            *time.Time   `json:"shared_cutoff_at,omitempty"`
	StabilizationRetriesUsed  int          `json:"stabilization_retries_used,omitempty"`
	PublisherDirtyQueueCount  int          `json:"publisher_dirty_queue_count,omitempty"`
	SubscriberDirtyQueueCount int          `json:"subscriber_dirty_queue_count,omitempty"`
}

type StableCompareMetadata struct {
	ConsistencyMode           string
	SnapshotStatus            string
	PublisherCapturedAt       *time.Time
	SubscriberCapturedAt      *time.Time
	SharedCutoffAt            *time.Time
	StabilizationRetriesUsed  int
	PublisherDirtyQueueCount  int
	SubscriberDirtyQueueCount int
}

type RowDiff struct {
	PKValue        string          `json:"pk_value"`
	PublisherData  json.RawMessage `json:"publisher_data,omitempty"`
	SubscriberData json.RawMessage `json:"subscriber_data,omitempty"`
	Status         string          `json:"status"`
	RepairSQL      string          `json:"repair_sql,omitempty"`
}

type InspectSummary struct {
	SchemaName         string    `json:"schema_name"`
	TableName          string    `json:"table_name"`
	PKColumn           string    `json:"pk_column"`
	BucketID           int64     `json:"bucket_id"`
	PKStart            int64     `json:"pk_start"`
	PKEnd              int64     `json:"pk_end"`
	PublisherRowCount  int       `json:"publisher_row_count"`
	SubscriberRowCount int       `json:"subscriber_row_count"`
	MismatchedRows     int       `json:"mismatched_rows"`
	Diffs              []RowDiff `json:"diffs"`
}

func CompareBuckets(publisher, subscriber []extension.BucketHash) Summary {
	pubByKey := make(map[BucketKey]extension.BucketHash, len(publisher))
	subByKey := make(map[BucketKey]extension.BucketHash, len(subscriber))
	allKeys := make(map[BucketKey]struct{}, len(publisher)+len(subscriber))

	for _, bucket := range publisher {
		key := BucketKey{SchemaName: bucket.SchemaName, TableName: bucket.TableName, BucketID: bucket.BucketID}
		pubByKey[key] = bucket
		allKeys[key] = struct{}{}
	}
	for _, bucket := range subscriber {
		key := BucketKey{SchemaName: bucket.SchemaName, TableName: bucket.TableName, BucketID: bucket.BucketID}
		subByKey[key] = bucket
		allKeys[key] = struct{}{}
	}

	keys := make([]BucketKey, 0, len(allKeys))
	for key := range allKeys {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].SchemaName != keys[j].SchemaName {
			return keys[i].SchemaName < keys[j].SchemaName
		}
		if keys[i].TableName != keys[j].TableName {
			return keys[i].TableName < keys[j].TableName
		}
		return keys[i].BucketID < keys[j].BucketID
	})

	summary := Summary{
		TotalBuckets:    len(keys),
		ComparedBuckets: len(keys),
		ConsistencyMode: "raw",
		SnapshotStatus:  "raw",
	}
	for _, key := range keys {
		pub, pubOK := pubByKey[key]
		sub, subOK := subByKey[key]

		switch {
		case pubOK && !subOK:
			summary.Diffs = append(summary.Diffs, BucketDiff{
				SchemaName:     key.SchemaName,
				TableName:      key.TableName,
				BucketID:       key.BucketID,
				PKStart:        &pub.PKStart,
				PKEnd:          &pub.PKEnd,
				PublisherHash:  pub.BucketHash,
				PublisherCount: &pub.RowCount,
				Status:         "missing_on_subscriber",
			})
		case !pubOK && subOK:
			summary.Diffs = append(summary.Diffs, BucketDiff{
				SchemaName:      key.SchemaName,
				TableName:       key.TableName,
				BucketID:        key.BucketID,
				PKStart:         &sub.PKStart,
				PKEnd:           &sub.PKEnd,
				SubscriberHash:  sub.BucketHash,
				SubscriberCount: &sub.RowCount,
				Status:          "missing_on_publisher",
			})
		default:
			status := classify(pub, sub)
			if status == "match" {
				continue
			}
			diff := BucketDiff{
				SchemaName:      key.SchemaName,
				TableName:       key.TableName,
				BucketID:        key.BucketID,
				PKStart:         chooseRange(pub.PKStart, sub.PKStart),
				PKEnd:           chooseRange(pub.PKEnd, sub.PKEnd),
				PublisherHash:   pub.BucketHash,
				SubscriberHash:  sub.BucketHash,
				PublisherCount:  &pub.RowCount,
				SubscriberCount: &sub.RowCount,
				Status:          status,
			}
			summary.Diffs = append(summary.Diffs, diff)
		}
	}

	summary.MismatchedBuckets = len(summary.Diffs)
	return summary
}

func CompareStableBuckets(
	publisherAll []extension.BucketHash,
	subscriberAll []extension.BucketHash,
	publisherStable []extension.BucketHash,
	subscriberStable []extension.BucketHash,
	metadata StableCompareMetadata,
) Summary {
	pubAllKeys := bucketSetKeys(publisherAll)
	subAllKeys := bucketSetKeys(subscriberAll)
	pubStableByKey := bucketSetMap(publisherStable)
	subStableByKey := bucketSetMap(subscriberStable)
	allKeys := unionKeys(pubAllKeys, subAllKeys)

	sort.Slice(allKeys, func(i, j int) bool {
		if allKeys[i].SchemaName != allKeys[j].SchemaName {
			return allKeys[i].SchemaName < allKeys[j].SchemaName
		}
		if allKeys[i].TableName != allKeys[j].TableName {
			return allKeys[i].TableName < allKeys[j].TableName
		}
		return allKeys[i].BucketID < allKeys[j].BucketID
	})

	summary := Summary{
		TotalBuckets:              len(allKeys),
		ConsistencyMode:           metadata.ConsistencyMode,
		SnapshotStatus:            metadata.SnapshotStatus,
		PublisherCapturedAt:       metadata.PublisherCapturedAt,
		SubscriberCapturedAt:      metadata.SubscriberCapturedAt,
		SharedCutoffAt:            metadata.SharedCutoffAt,
		StabilizationRetriesUsed:  metadata.StabilizationRetriesUsed,
		PublisherDirtyQueueCount:  metadata.PublisherDirtyQueueCount,
		SubscriberDirtyQueueCount: metadata.SubscriberDirtyQueueCount,
	}

	for _, key := range allKeys {
		pub, pubStable := pubStableByKey[key]
		sub, subStable := subStableByKey[key]

		if !pubStable || !subStable {
			summary.SkippedBuckets++
			if !pubStable {
				summary.SkippedOnPublisher++
			}
			if !subStable {
				summary.SkippedOnSubscriber++
			}
			continue
		}

		summary.ComparedBuckets++
		status := classify(pub, sub)
		if status == "match" {
			continue
		}

		diff := BucketDiff{
			SchemaName:      key.SchemaName,
			TableName:       key.TableName,
			BucketID:        key.BucketID,
			PKStart:         chooseRange(pub.PKStart, sub.PKStart),
			PKEnd:           chooseRange(pub.PKEnd, sub.PKEnd),
			PublisherHash:   pub.BucketHash,
			SubscriberHash:  sub.BucketHash,
			PublisherCount:  &pub.RowCount,
			SubscriberCount: &sub.RowCount,
			Status:          status,
		}
		summary.Diffs = append(summary.Diffs, diff)
	}

	summary.MismatchedBuckets = len(summary.Diffs)
	return summary
}

func classify(pub, sub extension.BucketHash) string {
	if pub.RowCount != sub.RowCount {
		return "count_mismatch"
	}

	pubHash := deref(pub.BucketHash)
	subHash := deref(sub.BucketHash)
	if pubHash != subHash {
		return "hash_mismatch"
	}

	return "match"
}

func chooseRange(a, b int64) *int64 {
	value := a
	if value == 0 {
		value = b
	}
	return &value
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (d BucketDiff) String() string {
	return fmt.Sprintf("%s.%s bucket=%d status=%s", d.SchemaName, d.TableName, d.BucketID, d.Status)
}

func InspectBucket(
	meta extension.MonitoredTable,
	bucketID int64,
	pkStart int64,
	pkEnd int64,
	publisherRows []extension.BucketRow,
	subscriberRows []extension.BucketRow,
) InspectSummary {
	pubByPK := make(map[string]extension.BucketRow, len(publisherRows))
	subByPK := make(map[string]extension.BucketRow, len(subscriberRows))
	allPKs := make(map[string]struct{}, len(publisherRows)+len(subscriberRows))

	for _, row := range publisherRows {
		pubByPK[row.PKValue] = row
		allPKs[row.PKValue] = struct{}{}
	}
	for _, row := range subscriberRows {
		subByPK[row.PKValue] = row
		allPKs[row.PKValue] = struct{}{}
	}

	pks := make([]string, 0, len(allPKs))
	for pk := range allPKs {
		pks = append(pks, pk)
	}
	sort.Strings(pks)

	summary := InspectSummary{
		SchemaName:         meta.SchemaName,
		TableName:          meta.TableName,
		PKColumn:           meta.PKColumn,
		BucketID:           bucketID,
		PKStart:            pkStart,
		PKEnd:              pkEnd,
		PublisherRowCount:  len(publisherRows),
		SubscriberRowCount: len(subscriberRows),
	}

	for _, pk := range pks {
		pub, pubOK := pubByPK[pk]
		sub, subOK := subByPK[pk]

		switch {
		case pubOK && !subOK:
			summary.Diffs = append(summary.Diffs, RowDiff{
				PKValue:       pk,
				PublisherData: cloneBytes(pub.RowData),
				Status:        "missing_on_subscriber",
			})
		case !pubOK && subOK:
			summary.Diffs = append(summary.Diffs, RowDiff{
				PKValue:        pk,
				SubscriberData: cloneBytes(sub.RowData),
				Status:         "missing_on_publisher",
			})
		case !bytes.Equal(pub.RowData, sub.RowData):
			summary.Diffs = append(summary.Diffs, RowDiff{
				PKValue:        pk,
				PublisherData:  cloneBytes(pub.RowData),
				SubscriberData: cloneBytes(sub.RowData),
				Status:         "row_mismatch",
			})
		}
	}

	summary.MismatchedRows = len(summary.Diffs)
	return summary
}

func cloneBytes(value []byte) json.RawMessage {
	if value == nil {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return json.RawMessage(out)
}

func bucketSetMap(buckets []extension.BucketHash) map[BucketKey]extension.BucketHash {
	byKey := make(map[BucketKey]extension.BucketHash, len(buckets))
	for _, bucket := range buckets {
		key := BucketKey{SchemaName: bucket.SchemaName, TableName: bucket.TableName, BucketID: bucket.BucketID}
		byKey[key] = bucket
	}
	return byKey
}

func bucketSetKeys(buckets []extension.BucketHash) []BucketKey {
	keys := make([]BucketKey, 0, len(buckets))
	for _, bucket := range buckets {
		keys = append(keys, BucketKey{SchemaName: bucket.SchemaName, TableName: bucket.TableName, BucketID: bucket.BucketID})
	}
	return keys
}

func unionKeys(left, right []BucketKey) []BucketKey {
	union := make(map[BucketKey]struct{}, len(left)+len(right))
	for _, key := range left {
		union[key] = struct{}{}
	}
	for _, key := range right {
		union[key] = struct{}{}
	}
	keys := make([]BucketKey, 0, len(union))
	for key := range union {
		keys = append(keys, key)
	}
	return keys
}
