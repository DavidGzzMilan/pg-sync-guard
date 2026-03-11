package compare

import (
	"fmt"
	"sort"

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
	TotalBuckets      int          `json:"total_buckets"`
	MismatchedBuckets int          `json:"mismatched_buckets"`
	Diffs             []BucketDiff `json:"diffs"`
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

	summary := Summary{TotalBuckets: len(keys)}
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
				SchemaName:       key.SchemaName,
				TableName:        key.TableName,
				BucketID:         key.BucketID,
				PKStart:          &sub.PKStart,
				PKEnd:            &sub.PKEnd,
				SubscriberHash:   sub.BucketHash,
				SubscriberCount:  &sub.RowCount,
				Status:           "missing_on_publisher",
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
