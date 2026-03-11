package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/compare"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/config"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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

	publisherBuckets, err := extension.FetchBucketCatalog(ctx, pubPool, cfg.Schema, cfg.Table)
	if err != nil {
		return fmt.Errorf("fetch publisher buckets: %w", err)
	}
	publisherName, err := extension.CurrentDatabaseName(ctx, pubPool)
	if err != nil {
		return fmt.Errorf("fetch publisher database name: %w", err)
	}
	subscriberBuckets, err := extension.FetchBucketCatalog(ctx, subPool, cfg.Schema, cfg.Table)
	if err != nil {
		return fmt.Errorf("fetch subscriber buckets: %w", err)
	}
	subscriberName, err := extension.CurrentDatabaseName(ctx, subPool)
	if err != nil {
		return fmt.Errorf("fetch subscriber database name: %w", err)
	}

	summary := compare.CompareBuckets(publisherBuckets, subscriberBuckets)

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
  SYNCGUARD_JSON
  SYNCGUARD_WRITE_CONTROL_PLANE`
}

func printUsage(w *os.File) {
	fmt.Fprintln(w, usageText())
}
