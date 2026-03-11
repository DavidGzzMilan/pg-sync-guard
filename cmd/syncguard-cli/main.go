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
	"github.com/DavidGzzMilan/pg-sync-guard/internal/report"
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

func usageError() error {
	return errors.New(usageText())
}

func usageText() string {
	return `Usage:
  syncguard-cli verify [flags]

Commands:
  verify    compare syncguard.bucket_catalog across publisher and subscriber

Flags for verify:
  --publisher-dsn         PostgreSQL DSN for publisher
  --subscriber-dsn        PostgreSQL DSN for subscriber
  --control-dsn           Optional PostgreSQL DSN for control plane
  --schema                Optional schema filter
  --table                 Optional table filter
  --json                  Emit JSON output
  --write-control-plane   Persist run results to control plane

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
