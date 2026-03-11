package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

type VerifyConfig struct {
	PublisherDSN  string
	SubscriberDSN string
	ControlDSN    string
	Schema        string
	Table         string
	JSON          bool
	WriteControl  bool
}

type InspectConfig struct {
	PublisherDSN  string
	SubscriberDSN string
	Schema        string
	Table         string
	BucketID      int64
	JSON          bool
}

type RepairConfig struct {
	PublisherDSN  string
	SubscriberDSN string
	Schema        string
	Table         string
	BucketID      int64
	JSON          bool
}

func NewVerifyCommand() (*flag.FlagSet, *VerifyConfig) {
	cfg := &VerifyConfig{}
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)

	fs.StringVar(&cfg.PublisherDSN, "publisher-dsn", getenv("SYNCGUARD_PUBLISHER_DSN", ""), "PostgreSQL DSN for the publisher")
	fs.StringVar(&cfg.SubscriberDSN, "subscriber-dsn", getenv("SYNCGUARD_SUBSCRIBER_DSN", ""), "PostgreSQL DSN for the subscriber")
	fs.StringVar(&cfg.ControlDSN, "control-dsn", getenv("SYNCGUARD_CONTROL_DSN", ""), "Optional PostgreSQL DSN for the control plane")
	fs.StringVar(&cfg.Schema, "schema", getenv("SYNCGUARD_SCHEMA", ""), "Optional schema filter")
	fs.StringVar(&cfg.Table, "table", getenv("SYNCGUARD_TABLE", ""), "Optional table filter")
	fs.BoolVar(&cfg.JSON, "json", getenvBool("SYNCGUARD_JSON", false), "Emit JSON instead of text output")
	fs.BoolVar(&cfg.WriteControl, "write-control-plane", getenvBool("SYNCGUARD_WRITE_CONTROL_PLANE", false), "Persist run results to the control plane")

	return fs, cfg
}

func (c *VerifyConfig) Validate() error {
	if strings.TrimSpace(c.PublisherDSN) == "" {
		return errors.New("publisher DSN is required")
	}
	if strings.TrimSpace(c.SubscriberDSN) == "" {
		return errors.New("subscriber DSN is required")
	}
	if c.WriteControl && strings.TrimSpace(c.ControlDSN) == "" {
		return errors.New("control DSN is required when --write-control-plane is set")
	}
	return nil
}

func NewInspectCommand() (*flag.FlagSet, *InspectConfig) {
	cfg := &InspectConfig{}
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)

	fs.StringVar(&cfg.PublisherDSN, "publisher-dsn", getenv("SYNCGUARD_PUBLISHER_DSN", ""), "PostgreSQL DSN for the publisher")
	fs.StringVar(&cfg.SubscriberDSN, "subscriber-dsn", getenv("SYNCGUARD_SUBSCRIBER_DSN", ""), "PostgreSQL DSN for the subscriber")
	fs.StringVar(&cfg.Schema, "schema", getenv("SYNCGUARD_SCHEMA", ""), "Schema name for the monitored table")
	fs.StringVar(&cfg.Table, "table", getenv("SYNCGUARD_TABLE", ""), "Table name for the monitored table")
	fs.Int64Var(&cfg.BucketID, "bucket-id", 0, "Bucket identifier to inspect")
	fs.BoolVar(&cfg.JSON, "json", getenvBool("SYNCGUARD_JSON", false), "Emit JSON instead of text output")

	return fs, cfg
}

func (c *InspectConfig) Validate() error {
	if strings.TrimSpace(c.PublisherDSN) == "" {
		return errors.New("publisher DSN is required")
	}
	if strings.TrimSpace(c.SubscriberDSN) == "" {
		return errors.New("subscriber DSN is required")
	}
	if strings.TrimSpace(c.Schema) == "" {
		return errors.New("schema is required")
	}
	if strings.TrimSpace(c.Table) == "" {
		return errors.New("table is required")
	}
	if c.BucketID < 0 {
		return errors.New("bucket-id must be zero or greater")
	}
	return nil
}

func NewRepairCommand() (*flag.FlagSet, *RepairConfig) {
	cfg := &RepairConfig{}
	fs := flag.NewFlagSet("repair", flag.ContinueOnError)

	fs.StringVar(&cfg.PublisherDSN, "publisher-dsn", getenv("SYNCGUARD_PUBLISHER_DSN", ""), "PostgreSQL DSN for the publisher")
	fs.StringVar(&cfg.SubscriberDSN, "subscriber-dsn", getenv("SYNCGUARD_SUBSCRIBER_DSN", ""), "PostgreSQL DSN for the subscriber")
	fs.StringVar(&cfg.Schema, "schema", getenv("SYNCGUARD_SCHEMA", ""), "Schema name for the monitored table")
	fs.StringVar(&cfg.Table, "table", getenv("SYNCGUARD_TABLE", ""), "Table name for the monitored table")
	fs.Int64Var(&cfg.BucketID, "bucket-id", 0, "Bucket identifier to repair")
	fs.BoolVar(&cfg.JSON, "json", getenvBool("SYNCGUARD_JSON", false), "Emit JSON instead of text output")

	return fs, cfg
}

func (c *RepairConfig) Validate() error {
	if strings.TrimSpace(c.PublisherDSN) == "" {
		return errors.New("publisher DSN is required")
	}
	if strings.TrimSpace(c.SubscriberDSN) == "" {
		return errors.New("subscriber DSN is required")
	}
	if strings.TrimSpace(c.Schema) == "" {
		return errors.New("schema is required")
	}
	if strings.TrimSpace(c.Table) == "" {
		return errors.New("table is required")
	}
	if c.BucketID < 0 {
		return errors.New("bucket-id must be zero or greater")
	}
	return nil
}

func (c *VerifyConfig) TableFilter() string {
	if c.Schema == "" && c.Table == "" {
		return ""
	}
	if c.Schema == "" {
		return c.Table
	}
	if c.Table == "" {
		return c.Schema + ".*"
	}
	return fmt.Sprintf("%s.%s", c.Schema, c.Table)
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}
