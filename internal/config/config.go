package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

type VerifyConfig struct {
	PublisherDSN      string
	SubscriberDSN     string
	ControlDSN        string
	Schema            string
	Table             string
	JSON              bool
	WriteControl      bool
	ConsistencyMode   string
	StabilityBufferMS int
	StabilityRetries  int
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
	fs.StringVar(&cfg.ConsistencyMode, "consistency-mode", getenv("SYNCGUARD_CONSISTENCY_MODE", "stable-watermark"), "Verification consistency mode: stable-watermark or raw")
	fs.IntVar(&cfg.StabilityBufferMS, "stability-buffer-ms", getenvInt("SYNCGUARD_STABILITY_BUFFER_MS", 250), "Extra safety buffer in milliseconds for stable-watermark mode")
	fs.IntVar(&cfg.StabilityRetries, "stability-retries", getenvInt("SYNCGUARD_STABILITY_RETRIES", 1), "Number of stabilization re-reads for stable-watermark mode")

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
	switch c.ConsistencyMode {
	case "raw", "stable-watermark":
	default:
		return errors.New("consistency-mode must be 'raw' or 'stable-watermark'")
	}
	if c.StabilityBufferMS < 0 {
		return errors.New("stability-buffer-ms must be zero or greater")
	}
	if c.StabilityRetries < 0 {
		return errors.New("stability-retries must be zero or greater")
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

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}
