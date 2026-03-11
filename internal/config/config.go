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
