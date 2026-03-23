package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/compare"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/config"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/consistency"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/controlplane"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/db"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/extension"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/repairplan"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/report"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "syncguard-cli: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usageError()
	}

	switch args[0] {
	case "verify":
		return runVerify(args[1:])
	case "inspect":
		return runInspect(args[1:])
	case "repair":
		return runRepair(args[1:])
	case "-h", "--help", "help":
		printUsage(os.Stdout)
		return nil
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], usageText())
	}
}

func runVerify(args []string) error {
	fs, cfg := config.NewVerifyCommand()
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	ctx, stop, cancel := commandContext()
	defer stop()
	defer cancel()

	pubPool, err := db.Connect(ctx, cfg.PublisherDSN)
	if err != nil {
		return fmt.Errorf("connect publisher: %w", err)
	}
	defer pubPool.Close()

	subPool, err := db.Connect(ctx, cfg.SubscriberDSN)
	if err != nil {
		return fmt.Errorf("connect subscriber: %w", err)
	}
	defer subPool.Close()

	publisherName, err := extension.CurrentDatabaseName(ctx, pubPool)
	if err != nil {
		return fmt.Errorf("fetch publisher database name: %w", err)
	}
	subscriberName, err := extension.CurrentDatabaseName(ctx, subPool)
	if err != nil {
		return fmt.Errorf("fetch subscriber database name: %w", err)
	}

	summary, err := verifySummary(ctx, pubPool, subPool, cfg)
	if err != nil {
		return err
	}

	if cfg.JSON {
		if err := report.WriteJSON(os.Stdout, publisherName, subscriberName, summary); err != nil {
			return fmt.Errorf("write JSON report: %w", err)
		}
	} else {
		if err := report.WriteText(os.Stdout, publisherName, subscriberName, summary); err != nil {
			return fmt.Errorf("write text report: %w", err)
		}
	}

	if cfg.WriteControl {
		controlPool, err := db.Connect(ctx, cfg.ControlDSN)
		if err != nil {
			return fmt.Errorf("connect control plane: %w", err)
		}
		defer controlPool.Close()

		if err := controlplane.WriteVerifyRun(
			ctx,
			controlPool,
			publisherName,
			subscriberName,
			cfg.TableFilter(),
			summary,
		); err != nil {
			return fmt.Errorf("write control plane records: %w", err)
		}
	}

	if summary.SnapshotStatus == "unstable_snapshot" {
		return errors.New("stable-watermark snapshot did not stabilize")
	}
	if summary.MismatchedBuckets > 0 {
		return errors.New("bucket mismatches found")
	}
	return nil
}

func runInspect(args []string) error {
	fs, cfg := config.NewInspectCommand()
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	ctx, stop, cancel := commandContext()
	defer stop()
	defer cancel()

	pubPool, subPool, err := connectPair(ctx, cfg.PublisherDSN, cfg.SubscriberDSN)
	if err != nil {
		return err
	}
	defer pubPool.Close()
	defer subPool.Close()

	publisherName, subscriberName, summary, err := inspectAndPlan(ctx, pubPool, subPool, cfg.Schema, cfg.Table, cfg.BucketID)
	if err != nil {
		return err
	}

	if cfg.JSON {
		if err := report.WriteInspectJSON(os.Stdout, publisherName, subscriberName, summary); err != nil {
			return fmt.Errorf("write JSON inspect report: %w", err)
		}
	} else {
		if err := report.WriteInspectText(os.Stdout, publisherName, subscriberName, summary); err != nil {
			return fmt.Errorf("write text inspect report: %w", err)
		}
	}

	if summary.MismatchedRows > 0 {
		return errors.New("row mismatches found in bucket")
	}
	return nil
}

func runRepair(args []string) error {
	fs, cfg := config.NewRepairCommand()
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	ctx, stop, cancel := commandContext()
	defer stop()
	defer cancel()

	pubPool, subPool, err := connectPair(ctx, cfg.PublisherDSN, cfg.SubscriberDSN)
	if err != nil {
		return err
	}
	defer pubPool.Close()
	defer subPool.Close()

	publisherName, subscriberName, inspectSummary, err := inspectAndPlan(ctx, pubPool, subPool, cfg.Schema, cfg.Table, cfg.BucketID)
	if err != nil {
		return err
	}

	applySummary, err := repairplan.ExecuteInspectPlans(ctx, subPool, inspectSummary)
	if err != nil {
		return fmt.Errorf("apply repair plan: %w", err)
	}

	if cfg.JSON {
		if err := report.WriteRepairJSON(os.Stdout, publisherName, subscriberName, applySummary); err != nil {
			return fmt.Errorf("write JSON repair report: %w", err)
		}
	} else {
		if err := report.WriteRepairText(os.Stdout, publisherName, subscriberName, applySummary); err != nil {
			return fmt.Errorf("write text repair report: %w", err)
		}
	}

	return nil
}

func commandContext() (context.Context, context.CancelFunc, context.CancelFunc) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	return ctx, stop, cancel
}

func connectPair(ctx context.Context, publisherDSN, subscriberDSN string) (*pgxpool.Pool, *pgxpool.Pool, error) {
	pubPool, err := db.Connect(ctx, publisherDSN)
	if err != nil {
		return nil, nil, fmt.Errorf("connect publisher: %w", err)
	}
	subPool, err := db.Connect(ctx, subscriberDSN)
	if err != nil {
		pubPool.Close()
		return nil, nil, fmt.Errorf("connect subscriber: %w", err)
	}
	return pubPool, subPool, nil
}

func inspectAndPlan(
	ctx context.Context,
	pubPool *pgxpool.Pool,
	subPool *pgxpool.Pool,
	schema string,
	table string,
	bucketID int64,
) (string, string, compare.InspectSummary, error) {
	pubMeta, err := extension.FetchMonitoredTable(ctx, pubPool, schema, table)
	if err != nil {
		return "", "", compare.InspectSummary{}, fmt.Errorf("fetch publisher monitored table metadata: %w", err)
	}
	subMeta, err := extension.FetchMonitoredTable(ctx, subPool, schema, table)
	if err != nil {
		return "", "", compare.InspectSummary{}, fmt.Errorf("fetch subscriber monitored table metadata: %w", err)
	}
	if pubMeta.PKColumn != subMeta.PKColumn {
		return "", "", compare.InspectSummary{}, fmt.Errorf("pk column mismatch between publisher (%s) and subscriber (%s)", pubMeta.PKColumn, subMeta.PKColumn)
	}
	if pubMeta.BucketSize != subMeta.BucketSize {
		return "", "", compare.InspectSummary{}, fmt.Errorf("bucket size mismatch between publisher (%d) and subscriber (%d)", pubMeta.BucketSize, subMeta.BucketSize)
	}

	publisherRows, pkStart, pkEnd, err := extension.FetchBucketRows(ctx, pubPool, pubMeta, bucketID)
	if err != nil {
		return "", "", compare.InspectSummary{}, fmt.Errorf("fetch publisher bucket rows: %w", err)
	}
	subscriberRows, _, _, err := extension.FetchBucketRows(ctx, subPool, subMeta, bucketID)
	if err != nil {
		return "", "", compare.InspectSummary{}, fmt.Errorf("fetch subscriber bucket rows: %w", err)
	}

	publisherName, err := extension.CurrentDatabaseName(ctx, pubPool)
	if err != nil {
		return "", "", compare.InspectSummary{}, fmt.Errorf("fetch publisher database name: %w", err)
	}
	subscriberName, err := extension.CurrentDatabaseName(ctx, subPool)
	if err != nil {
		return "", "", compare.InspectSummary{}, fmt.Errorf("fetch subscriber database name: %w", err)
	}

	columns, err := extension.FetchTableColumns(ctx, pubPool, schema, table)
	if err != nil {
		return "", "", compare.InspectSummary{}, fmt.Errorf("fetch publisher table columns: %w", err)
	}

	summary := compare.InspectBucket(pubMeta, bucketID, pkStart, pkEnd, publisherRows, subscriberRows)
	if err := repairplan.ApplyInspectPlans(&summary, pubMeta, columns); err != nil {
		return "", "", compare.InspectSummary{}, fmt.Errorf("build repair plan: %w", err)
	}

	return publisherName, subscriberName, summary, nil
}

func verifySummary(ctx context.Context, pubPool, subPool *pgxpool.Pool, cfg *config.VerifyConfig) (compare.Summary, error) {
	switch cfg.ConsistencyMode {
	case consistency.ModeRaw:
		publisherBuckets, err := extension.FetchBucketCatalog(ctx, pubPool, cfg.Schema, cfg.Table)
		if err != nil {
			return compare.Summary{}, fmt.Errorf("fetch publisher buckets: %w", err)
		}
		subscriberBuckets, err := extension.FetchBucketCatalog(ctx, subPool, cfg.Schema, cfg.Table)
		if err != nil {
			return compare.Summary{}, fmt.Errorf("fetch subscriber buckets: %w", err)
		}
		return compare.CompareBuckets(publisherBuckets, subscriberBuckets), nil
	case consistency.ModeStableWatermark:
		return verifyStableWatermark(ctx, pubPool, subPool, cfg)
	default:
		return compare.Summary{}, fmt.Errorf("unsupported consistency mode: %s", cfg.ConsistencyMode)
	}
}

func verifyStableWatermark(ctx context.Context, pubPool, subPool *pgxpool.Pool, cfg *config.VerifyConfig) (compare.Summary, error) {
	var previous *verifyRead

	for attempt := 0; attempt <= cfg.StabilityRetries; attempt++ {
		current, err := readStableWatermarkSnapshot(ctx, pubPool, subPool, cfg)
		if err != nil {
			return compare.Summary{}, err
		}

		if previous == nil {
			if cfg.StabilityRetries == 0 {
				summary, err := finalizeStableSummary(ctx, pubPool, subPool, cfg, current, 0, true)
				if err != nil {
					return compare.Summary{}, err
				}
				return summary, nil
			}
			previous = &current
			continue
		}

		if consistency.EqualEligibleBuckets(previous.publisherStable, current.publisherStable) &&
			consistency.EqualEligibleBuckets(previous.subscriberStable, current.subscriberStable) {
			summary, err := finalizeStableSummary(ctx, pubPool, subPool, cfg, current, attempt, true)
			if err != nil {
				return compare.Summary{}, err
			}
			return summary, nil
		}

		previous = &current
	}

	if previous == nil {
		return compare.Summary{}, errors.New("failed to collect stable-watermark snapshot")
	}
	summary, err := finalizeStableSummary(ctx, pubPool, subPool, cfg, *previous, cfg.StabilityRetries, false)
	if err != nil {
		return compare.Summary{}, err
	}
	return summary, nil
}

func readStableWatermarkSnapshot(ctx context.Context, pubPool, subPool *pgxpool.Pool, cfg *config.VerifyConfig) (verifyRead, error) {
	var snapshot verifyRead

	var err error
	snapshot.publisherMetadata, err = extension.FetchVerificationMetadata(ctx, pubPool, cfg.Schema, cfg.Table)
	if err != nil {
		return snapshot, fmt.Errorf("fetch publisher verification metadata: %w", err)
	}
	snapshot.subscriberMetadata, err = extension.FetchVerificationMetadata(ctx, subPool, cfg.Schema, cfg.Table)
	if err != nil {
		return snapshot, fmt.Errorf("fetch subscriber verification metadata: %w", err)
	}

	snapshot.window = consistency.ComputeWatermark(
		consistency.SideSnapshot{
			CapturedAt:        snapshot.publisherMetadata.DatabaseTime,
			NaptimeMS:         snapshot.publisherMetadata.NaptimeMS,
			DirtyQueueCount:   snapshot.publisherMetadata.DirtyQueueCount,
			OldestDirtyQueued: snapshot.publisherMetadata.OldestDirtyQueuedAt,
		},
		consistency.SideSnapshot{
			CapturedAt:        snapshot.subscriberMetadata.DatabaseTime,
			NaptimeMS:         snapshot.subscriberMetadata.NaptimeMS,
			DirtyQueueCount:   snapshot.subscriberMetadata.DirtyQueueCount,
			OldestDirtyQueued: snapshot.subscriberMetadata.OldestDirtyQueuedAt,
		},
		time.Duration(cfg.StabilityBufferMS)*time.Millisecond,
	)

	snapshot.publisherBuckets, err = extension.FetchBucketCatalog(ctx, pubPool, cfg.Schema, cfg.Table)
	if err != nil {
		return snapshot, fmt.Errorf("fetch publisher buckets: %w", err)
	}
	snapshot.subscriberBuckets, err = extension.FetchBucketCatalog(ctx, subPool, cfg.Schema, cfg.Table)
	if err != nil {
		return snapshot, fmt.Errorf("fetch subscriber buckets: %w", err)
	}
	snapshot.publisherStable, err = extension.FetchStableBucketCatalog(ctx, pubPool, cfg.Schema, cfg.Table, snapshot.window.SharedCutoffAt)
	if err != nil {
		return snapshot, fmt.Errorf("fetch publisher stable buckets: %w", err)
	}
	snapshot.subscriberStable, err = extension.FetchStableBucketCatalog(ctx, subPool, cfg.Schema, cfg.Table, snapshot.window.SharedCutoffAt)
	if err != nil {
		return snapshot, fmt.Errorf("fetch subscriber stable buckets: %w", err)
	}

	return snapshot, nil
}

func stableSummaryFromRead(read verifyRead,
	retriesUsed int,
	stable bool,
) compare.Summary {
	publisherCapturedAt := read.publisherMetadata.DatabaseTime
	subscriberCapturedAt := read.subscriberMetadata.DatabaseTime
	sharedCutoff := read.window.SharedCutoffAt
	status := "stable"
	if !stable {
		status = "unstable_snapshot"
	}

	return compare.CompareStableBuckets(
		read.publisherBuckets,
		read.subscriberBuckets,
		read.publisherStable,
		read.subscriberStable,
		compare.StableCompareMetadata{
			ConsistencyMode:           consistency.ModeStableWatermark,
			SnapshotStatus:            status,
			PublisherCapturedAt:       &publisherCapturedAt,
			SubscriberCapturedAt:      &subscriberCapturedAt,
			SharedCutoffAt:            &sharedCutoff,
			StabilizationRetriesUsed:  retriesUsed,
			PublisherDirtyQueueCount:  read.publisherMetadata.DirtyQueueCount,
			SubscriberDirtyQueueCount: read.subscriberMetadata.DirtyQueueCount,
		},
	)
}

func finalizeStableSummary(
	ctx context.Context,
	pubPool, subPool *pgxpool.Pool,
	cfg *config.VerifyConfig,
	read verifyRead,
	retriesUsed int,
	stable bool,
) (compare.Summary, error) {
	summary := stableSummaryFromRead(read, retriesUsed, stable)

	if cfg.LiveFallback && summary.SkippedBuckets > 0 {
		liveApplied, refreshedSummary, err := applyLiveFallback(ctx, pubPool, subPool, cfg, read, summary)
		if err != nil {
			return compare.Summary{}, err
		}
		summary = refreshedSummary
		summary.LiveFallbackBuckets = liveApplied
	}

	compare.FinalizeCoverage(&summary, cfg.MinCoveragePct)
	return summary, nil
}

func applyLiveFallback(
	ctx context.Context,
	pubPool, subPool *pgxpool.Pool,
	cfg *config.VerifyConfig,
	read verifyRead,
	summary compare.Summary,
) (int, compare.Summary, error) {
	olderThan := read.window.SharedAnchorAt.Add(-time.Duration(cfg.LiveFallbackAgeMS) * time.Millisecond)

	pubDirty, err := extension.FetchDirtyBucketsOlderThan(ctx, pubPool, cfg.Schema, cfg.Table, olderThan)
	if err != nil {
		return 0, compare.Summary{}, fmt.Errorf("fetch publisher dirty buckets for live fallback: %w", err)
	}
	subDirty, err := extension.FetchDirtyBucketsOlderThan(ctx, subPool, cfg.Schema, cfg.Table, olderThan)
	if err != nil {
		return 0, compare.Summary{}, fmt.Errorf("fetch subscriber dirty buckets for live fallback: %w", err)
	}

	skippedKeys := skippedBucketKeys(read.publisherBuckets, read.subscriberBuckets, read.publisherStable, read.subscriberStable)
	if len(skippedKeys) == 0 {
		return 0, summary, nil
	}

	candidateKeys := dirtyCandidateKeys(pubDirty, subDirty)
	pubMerged := bucketMapFromSlice(read.publisherStable)
	subMerged := bucketMapFromSlice(read.subscriberStable)
	metaCachePub := make(map[string]extension.MonitoredTable)
	metaCacheSub := make(map[string]extension.MonitoredTable)
	applied := 0

	for key, dirty := range candidateKeys {
		if _, ok := skippedKeys[key]; !ok {
			continue
		}

		pubMeta, subMeta, err := loadMonitoredMetaPair(ctx, pubPool, subPool, metaCachePub, metaCacheSub, dirty.SchemaName, dirty.TableName)
		if err != nil {
			return 0, compare.Summary{}, err
		}

		pubLive, err := extension.FetchLiveBucketHash(ctx, pubPool, pubMeta, dirty.PKStart, dirty.PKEnd)
		if err != nil {
			return 0, compare.Summary{}, fmt.Errorf("fetch publisher live bucket hash for %s.%s bucket=%d: %w", dirty.SchemaName, dirty.TableName, dirty.BucketID, err)
		}
		subLive, err := extension.FetchLiveBucketHash(ctx, subPool, subMeta, dirty.PKStart, dirty.PKEnd)
		if err != nil {
			return 0, compare.Summary{}, fmt.Errorf("fetch subscriber live bucket hash for %s.%s bucket=%d: %w", dirty.SchemaName, dirty.TableName, dirty.BucketID, err)
		}

		pubMerged[key] = pubLive
		subMerged[key] = subLive
		applied++
	}

	if applied == 0 {
		return 0, summary, nil
	}

	refreshed := compare.CompareStableBuckets(
		read.publisherBuckets,
		read.subscriberBuckets,
		bucketSliceFromMap(pubMerged),
		bucketSliceFromMap(subMerged),
		compare.StableCompareMetadata{
			ConsistencyMode:           summary.ConsistencyMode,
			SnapshotStatus:            summary.SnapshotStatus,
			PublisherCapturedAt:       summary.PublisherCapturedAt,
			SubscriberCapturedAt:      summary.SubscriberCapturedAt,
			SharedCutoffAt:            summary.SharedCutoffAt,
			StabilizationRetriesUsed:  summary.StabilizationRetriesUsed,
			PublisherDirtyQueueCount:  summary.PublisherDirtyQueueCount,
			SubscriberDirtyQueueCount: summary.SubscriberDirtyQueueCount,
		},
	)

	return applied, refreshed, nil
}

func skippedBucketKeys(
	publisherAll []extension.BucketHash,
	subscriberAll []extension.BucketHash,
	publisherStable []extension.BucketHash,
	subscriberStable []extension.BucketHash,
) map[compare.BucketKey]struct{} {
	all := make(map[compare.BucketKey]struct{}, len(publisherAll)+len(subscriberAll))
	stable := make(map[compare.BucketKey]struct{}, len(publisherStable)+len(subscriberStable))

	for _, bucket := range publisherAll {
		all[bucketKey(bucket)] = struct{}{}
	}
	for _, bucket := range subscriberAll {
		all[bucketKey(bucket)] = struct{}{}
	}
	for _, bucket := range publisherStable {
		stable[bucketKey(bucket)] = struct{}{}
	}
	for _, bucket := range subscriberStable {
		stable[bucketKey(bucket)] = struct{}{}
	}

	skipped := make(map[compare.BucketKey]struct{})
	for key := range all {
		if _, ok := stable[key]; ok {
			continue
		}
		skipped[key] = struct{}{}
	}
	return skipped
}

func dirtyCandidateKeys(publisher, subscriber []extension.DirtyBucket) map[compare.BucketKey]extension.DirtyBucket {
	candidates := make(map[compare.BucketKey]extension.DirtyBucket, len(publisher)+len(subscriber))
	for _, dirty := range publisher {
		candidates[dirtyBucketKey(dirty)] = dirty
	}
	for _, dirty := range subscriber {
		key := dirtyBucketKey(dirty)
		if existing, ok := candidates[key]; ok {
			if dirty.QueuedAt.Before(existing.QueuedAt) {
				candidates[key] = dirty
			}
			continue
		}
		candidates[key] = dirty
	}
	return candidates
}

func loadMonitoredMetaPair(
	ctx context.Context,
	pubPool, subPool *pgxpool.Pool,
	pubCache, subCache map[string]extension.MonitoredTable,
	schema, table string,
) (extension.MonitoredTable, extension.MonitoredTable, error) {
	cacheKey := schema + "." + table
	pubMeta, ok := pubCache[cacheKey]
	if !ok {
		var err error
		pubMeta, err = extension.FetchMonitoredTable(ctx, pubPool, schema, table)
		if err != nil {
			return extension.MonitoredTable{}, extension.MonitoredTable{}, fmt.Errorf("fetch publisher metadata for %s: %w", cacheKey, err)
		}
		pubCache[cacheKey] = pubMeta
	}

	subMeta, ok := subCache[cacheKey]
	if !ok {
		var err error
		subMeta, err = extension.FetchMonitoredTable(ctx, subPool, schema, table)
		if err != nil {
			return extension.MonitoredTable{}, extension.MonitoredTable{}, fmt.Errorf("fetch subscriber metadata for %s: %w", cacheKey, err)
		}
		subCache[cacheKey] = subMeta
	}

	if pubMeta.PKColumn != subMeta.PKColumn {
		return extension.MonitoredTable{}, extension.MonitoredTable{}, fmt.Errorf("pk column mismatch between publisher and subscriber for %s", cacheKey)
	}
	if pubMeta.BucketSize != subMeta.BucketSize {
		return extension.MonitoredTable{}, extension.MonitoredTable{}, fmt.Errorf("bucket size mismatch between publisher and subscriber for %s", cacheKey)
	}

	return pubMeta, subMeta, nil
}

func bucketMapFromSlice(buckets []extension.BucketHash) map[compare.BucketKey]extension.BucketHash {
	m := make(map[compare.BucketKey]extension.BucketHash, len(buckets))
	for _, bucket := range buckets {
		m[bucketKey(bucket)] = bucket
	}
	return m
}

func bucketSliceFromMap(m map[compare.BucketKey]extension.BucketHash) []extension.BucketHash {
	keys := make([]compare.BucketKey, 0, len(m))
	for key := range m {
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

	out := make([]extension.BucketHash, 0, len(keys))
	for _, key := range keys {
		out = append(out, m[key])
	}
	return out
}

func bucketKey(bucket extension.BucketHash) compare.BucketKey {
	return compare.BucketKey{
		SchemaName: bucket.SchemaName,
		TableName:  bucket.TableName,
		BucketID:   bucket.BucketID,
	}
}

func dirtyBucketKey(bucket extension.DirtyBucket) compare.BucketKey {
	return compare.BucketKey{
		SchemaName: bucket.SchemaName,
		TableName:  bucket.TableName,
		BucketID:   bucket.BucketID,
	}
}

type verifyRead struct {
	publisherMetadata  extension.VerificationMetadata
	subscriberMetadata extension.VerificationMetadata
	publisherBuckets   []extension.BucketHash
	subscriberBuckets  []extension.BucketHash
	publisherStable    []extension.BucketHash
	subscriberStable   []extension.BucketHash
	window             consistency.Watermark
}

func usageError() error {
	return errors.New(usageText())
}

func usageText() string {
	return `Usage:
  syncguard-cli <command> [flags]

Commands:
  verify    compare syncguard.bucket_catalog across publisher and subscriber
  inspect   compare full rows inside one bucket range
  repair    apply planned repair SQL to the subscriber for one bucket

Flags for verify:
  --publisher-dsn         PostgreSQL DSN for publisher
  --subscriber-dsn        PostgreSQL DSN for subscriber
  --control-dsn           Optional PostgreSQL DSN for control plane
  --schema                Optional schema filter
  --table                 Optional table filter
  --consistency-mode      Verification consistency mode: stable-watermark or raw
  --stability-buffer-ms   Extra safety buffer in milliseconds for stable-watermark
  --stability-retries     Number of stabilization re-reads for stable-watermark
  --min-coverage-pct      Warn when compared bucket coverage falls below this percentage
  --live-fallback-for-dirty
                          Aggressively verify long-dirty buckets with live hashing
  --live-fallback-dirty-age-ms
                          Minimum dirty age before live fallback is attempted
  --json                  Emit JSON output
  --write-control-plane   Persist run results to control plane

Flags for inspect:
  --publisher-dsn         PostgreSQL DSN for publisher
  --subscriber-dsn        PostgreSQL DSN for subscriber
  --schema                Schema name for the monitored table
  --table                 Table name for the monitored table
  --bucket-id             Bucket identifier to inspect
  --json                  Emit JSON output

Flags for repair:
  --publisher-dsn         PostgreSQL DSN for publisher
  --subscriber-dsn        PostgreSQL DSN for subscriber
  --schema                Schema name for the monitored table
  --table                 Table name for the monitored table
  --bucket-id             Bucket identifier to repair
  --json                  Emit JSON output

Environment variables:
  SYNCGUARD_PUBLISHER_DSN
  SYNCGUARD_SUBSCRIBER_DSN
  SYNCGUARD_CONTROL_DSN
  SYNCGUARD_SCHEMA
  SYNCGUARD_TABLE
  SYNCGUARD_CONSISTENCY_MODE
  SYNCGUARD_STABILITY_BUFFER_MS
  SYNCGUARD_STABILITY_RETRIES
  SYNCGUARD_MIN_COVERAGE_PCT
  SYNCGUARD_LIVE_FALLBACK_FOR_DIRTY
  SYNCGUARD_LIVE_FALLBACK_DIRTY_AGE_MS
  SYNCGUARD_JSON
  SYNCGUARD_WRITE_CONTROL_PLANE`
}

func printUsage(w *os.File) {
	fmt.Fprintln(w, usageText())
}
