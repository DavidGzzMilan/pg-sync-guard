package repairplan

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/DavidGzzMilan/pg-sync-guard/internal/compare"
	"github.com/DavidGzzMilan/pg-sync-guard/internal/extension"
)

func ApplyInspectPlans(summary *compare.InspectSummary, meta extension.MonitoredTable, columns []extension.TableColumn) error {
	for i := range summary.Diffs {
		sql, err := BuildRepairSQL(meta, columns, summary.Diffs[i])
		if err != nil {
			return err
		}
		summary.Diffs[i].RepairSQL = sql
	}
	return nil
}

func BuildRepairSQL(meta extension.MonitoredTable, columns []extension.TableColumn, diff compare.RowDiff) (string, error) {
	switch diff.Status {
	case "missing_on_subscriber", "row_mismatch":
		if len(diff.PublisherData) == 0 {
			return "", fmt.Errorf("publisher row data required for status %s", diff.Status)
		}
		return buildUpsertSQL(meta, columns, diff.PublisherData)
	case "missing_on_publisher":
		if len(diff.SubscriberData) == 0 {
			return "", fmt.Errorf("subscriber row data required for status %s", diff.Status)
		}
		return buildDeleteSQL(meta, columns, diff.SubscriberData)
	default:
		return "", nil
	}
}

func buildUpsertSQL(meta extension.MonitoredTable, columns []extension.TableColumn, rowData json.RawMessage) (string, error) {
	insertColumns := make([]extension.TableColumn, 0, len(columns))
	updateAssignments := make([]string, 0, len(columns))
	recordColumns := make([]string, 0, len(columns))

	hasIdentityAlways := false
	for _, column := range columns {
		if column.IsGenerated {
			continue
		}
		insertColumns = append(insertColumns, column)
		recordColumns = append(recordColumns, fmt.Sprintf("%s %s", quoteIdentifier(column.Name), column.TypeSQL))
		if column.IsIdentityAlways {
			hasIdentityAlways = true
		}
		if column.Name == meta.PKColumn || column.IsIdentityAlways {
			continue
		}
		updateAssignments = append(updateAssignments, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdentifier(column.Name), quoteIdentifier(column.Name)))
	}

	if len(insertColumns) == 0 {
		return "", fmt.Errorf("no insertable columns found for %s.%s", meta.SchemaName, meta.TableName)
	}

	insertList := joinQuotedColumnNames(insertColumns)
	selectList := prefixedQuotedColumnNames(insertColumns, "src")
	overrideClause := ""
	if hasIdentityAlways {
		overrideClause = "OVERRIDING SYSTEM VALUE\n"
	}

	conflictClause := fmt.Sprintf("ON CONFLICT (%s) DO NOTHING", quoteIdentifier(meta.PKColumn))
	if len(updateAssignments) > 0 {
		conflictClause = fmt.Sprintf(
			"ON CONFLICT (%s) DO UPDATE SET\n    %s",
			quoteIdentifier(meta.PKColumn),
			strings.Join(updateAssignments, ",\n    "),
		)
	}

	return fmt.Sprintf(
		"INSERT INTO %s (\n    %s\n)\n%sSELECT\n    %s\nFROM jsonb_to_record(%s::jsonb) AS src(%s)\n%s;",
		quoteQualifiedIdentifier(meta.SchemaName, meta.TableName),
		insertList,
		overrideClause,
		selectList,
		quoteLiteral(string(rowData)),
		strings.Join(recordColumns, ", "),
		conflictClause,
	), nil
}

func buildDeleteSQL(meta extension.MonitoredTable, columns []extension.TableColumn, rowData json.RawMessage) (string, error) {
	pkColumn, ok := findColumn(columns, meta.PKColumn)
	if !ok {
		return "", fmt.Errorf("pk column %s not found for %s.%s", meta.PKColumn, meta.SchemaName, meta.TableName)
	}

	return fmt.Sprintf(
		"DELETE FROM %s AS tgt\nUSING (\n    SELECT src.%s AS %s\n    FROM jsonb_to_record(%s::jsonb) AS src(%s %s)\n) AS payload\nWHERE tgt.%s = payload.%s;",
		quoteQualifiedIdentifier(meta.SchemaName, meta.TableName),
		quoteIdentifier(pkColumn.Name),
		quoteIdentifier(pkColumn.Name),
		quoteLiteral(string(rowData)),
		quoteIdentifier(pkColumn.Name),
		pkColumn.TypeSQL,
		quoteIdentifier(pkColumn.Name),
		quoteIdentifier(pkColumn.Name),
	), nil
}

func findColumn(columns []extension.TableColumn, name string) (extension.TableColumn, bool) {
	for _, column := range columns {
		if column.Name == name {
			return column, true
		}
	}
	return extension.TableColumn{}, false
}

func joinQuotedColumnNames(columns []extension.TableColumn) string {
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, quoteIdentifier(column.Name))
	}
	return strings.Join(parts, ",\n    ")
}

func prefixedQuotedColumnNames(columns []extension.TableColumn, prefix string) string {
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, fmt.Sprintf("%s.%s", prefix, quoteIdentifier(column.Name)))
	}
	return strings.Join(parts, ",\n    ")
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func quoteQualifiedIdentifier(schemaName, tableName string) string {
	return quoteIdentifier(schemaName) + "." + quoteIdentifier(tableName)
}

func quoteLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
