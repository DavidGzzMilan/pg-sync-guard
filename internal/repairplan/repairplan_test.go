package repairplan

import (
	"strings"
	"testing"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/compare"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/extension"
)

func TestBuildRepairSQLUpsert(t *testing.T) {
	meta := extension.MonitoredTable{
		SchemaName: "public",
		TableName:  "items",
		PKColumn:   "id",
	}
	columns := []extension.TableColumn{
		{Name: "id", TypeSQL: "bigint"},
		{Name: "payload", TypeSQL: "text"},
	}

	sql, err := BuildRepairSQL(meta, columns, compare.RowDiff{
		Status:        "row_mismatch",
		PublisherData: []byte(`{"id":401,"payload":"fixed"}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, `INSERT INTO "public"."items"`) {
		t.Fatalf("expected insert statement, got %s", sql)
	}
	if !strings.Contains(sql, `ON CONFLICT ("id") DO UPDATE SET`) {
		t.Fatalf("expected upsert conflict clause, got %s", sql)
	}
	if !strings.Contains(sql, `jsonb_to_record('{"id":401,"payload":"fixed"}'::jsonb)`) {
		t.Fatalf("expected jsonb_to_record payload, got %s", sql)
	}
}

func TestBuildRepairSQLDelete(t *testing.T) {
	meta := extension.MonitoredTable{
		SchemaName: "public",
		TableName:  "items",
		PKColumn:   "id",
	}
	columns := []extension.TableColumn{
		{Name: "id", TypeSQL: "bigint"},
		{Name: "payload", TypeSQL: "text"},
	}

	sql, err := BuildRepairSQL(meta, columns, compare.RowDiff{
		Status:         "missing_on_publisher",
		SubscriberData: []byte(`{"id":403,"payload":"extra"}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(sql, `DELETE FROM "public"."items" AS tgt`) {
		t.Fatalf("expected delete statement, got %s", sql)
	}
	if !strings.Contains(sql, `WHERE tgt."id" = payload."id"`) {
		t.Fatalf("expected delete predicate, got %s", sql)
	}
}

func TestApplyInspectPlans(t *testing.T) {
	meta := extension.MonitoredTable{
		SchemaName: "public",
		TableName:  "items",
		PKColumn:   "id",
	}
	columns := []extension.TableColumn{
		{Name: "id", TypeSQL: "bigint"},
		{Name: "payload", TypeSQL: "text"},
	}

	summary := compare.InspectSummary{
		Diffs: []compare.RowDiff{
			{PKValue: "1", Status: "missing_on_subscriber", PublisherData: []byte(`{"id":1,"payload":"a"}`)},
			{PKValue: "2", Status: "missing_on_publisher", SubscriberData: []byte(`{"id":2,"payload":"b"}`)},
		},
	}

	if err := ApplyInspectPlans(&summary, meta, columns); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if summary.Diffs[0].RepairSQL == "" || summary.Diffs[1].RepairSQL == "" {
		t.Fatalf("expected repair sql for all diffs, got %#v", summary.Diffs)
	}
}
