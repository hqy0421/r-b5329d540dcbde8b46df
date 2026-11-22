package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"

	goparquet "github.com/parquet-go/parquet-go"
)

// opTruncate mirrors stream.go: a delta row with __op="T" is a TRUNCATE
// boundary — the whole relation was emptied at that epoch.
const opTruncate = "T"

// Compact merges delta Parquet files into a new base snapshot for each table.
// Applies INSERT/UPDATE/DELETE/TRUNCATE operations, maintains tombstones with TTL.
func Compact(ctx context.Context, sink ports.Sink) error {
	return CompactTables(ctx, sink, nil)
}

// CompactTables compacts only the named tables (qualified names). An empty
// list compacts every active table, which is the behaviour of `pg-cdc
// compact`. The streaming path passes the tables whose latest delta carries
// a TRUNCATE boundary so their base converges immediately.
func CompactTables(ctx context.Context, sink ports.Sink, tableNames []string) error {
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	active := m.ActiveTables()
	targets := active
	if len(tableNames) > 0 {
		targets = make(map[string]manifest.Table, len(tableNames))
		for _, name := range tableNames {
			t, ok := active[name]
			if !ok {
				fmt.Printf("  %s: skip (not an active table in manifest)\n", name)
				continue
			}
			targets[name] = t
		}
	}

	fmt.Printf("Compacting %d tables\n", len(targets))

	for tableName, table := range targets {
		if table.LatestDeltaEpoch <= table.BaseEpoch {
			fmt.Printf("  %s: up to date (base=%d, latest=%d)\n", tableName, table.BaseEpoch, table.LatestDeltaEpoch)
			continue
		}

		schema, ok := m.Schemas[tableName]
		if !ok {
			fmt.Printf("  %s: skip (no schema)\n", tableName)
			continue
		}

		if err := compactTable(ctx, sink, m, tableName, table, schema); err != nil {
			return fmt.Errorf("compact %s: %w", tableName, err)
		}
	}

	// Write updated manifest
	m.Compaction.LastCompactedAt = time.Now().UTC()
	m.UpdatedAt = time.Now().UTC()
	if err := sink.WriteManifest(ctx, m); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	fmt.Println("Compaction complete")
	return nil
}

func compactTable(ctx context.Context, sink ports.Sink, m *manifest.Manifest, tableName string, table manifest.Table, schema manifest.Schema) error {
	// Step 1: Read current base into memory (keyed by PK)
	rows, err := readBase(ctx, sink, table)
	if err != nil {
		return fmt.Errorf("read base: %w", err)
	}

	pkCols := table.PrimaryKey
	if len(pkCols) == 0 {
		return fmt.Errorf("no primary key for %s", tableName)
	}

	// Index by PK
	indexed := make(map[string]writer.Row, len(rows))
	for _, row := range rows {
		key := pkKey(row, pkCols)
		indexed[key] = row
	}

	// Step 2: Read and apply each delta
	deltaPath := table.Path + "/deltas/"
	deltaFiles, err := sink.ListFiles(ctx, deltaPath)
	if err != nil {
		return fmt.Errorf("list deltas: %w", err)
	}

	newDeltas, err := epoch.FilterAfter(deltaFiles, table.BaseEpoch)
	if err != nil {
		return err
	}

	if len(newDeltas) == 0 {
		return nil
	}

	var applied, truncated int
	truncateAt := time.Now().UTC()
	for _, deltaFile := range newDeltas {
		deltaRows, err := readParquetRows(ctx, sink, deltaFile)
		if err != nil {
			return fmt.Errorf("read delta %s: %w", deltaFile, err)
		}

		for _, dRow := range deltaRows {
			op, _ := dRow[btypes.ColOp].(string)
			key := pkKey(dRow, pkCols)

			switch op {
			case "I", "U":
				clean := make(writer.Row, len(dRow))
				for k, v := range dRow {
					if k == btypes.ColOp || k == btypes.ColEpoch {
						continue
					}
					clean[k] = v
				}
				// A re-insert after a TRUNCATE (or after a row-level D)
				// must resurrect the row: never carry a stale tombstone
				// into the new base, or the TTL purge would remove live
				// data 30 days later.
				delete(clean, btypes.ColDeletedAt)
				indexed[key] = clean
			case "D":
				if existing, ok := indexed[key]; ok {
					existing[btypes.ColDeletedAt] = time.Now().UTC().Format(time.RFC3339)
					indexed[key] = existing
				}
			case opTruncate:
				// TRUNCATE empties the whole relation. Every row still
				// visible at this boundary becomes a tombstone rather than
				// being hard-deleted: the existing 30-day TTL then purges
				// them, and rows already carrying __deleted_at keep their
				// original timestamp. Rows inserted/updated AFTER this
				// boundary land in `indexed` via the I/U cases below and so
				// survive — this is what makes re-imported data visible.
				for k, existing := range indexed {
					if _, tombstoned := existing[btypes.ColDeletedAt]; !tombstoned {
						existing[btypes.ColDeletedAt] = truncateAt.Format(time.RFC3339)
						indexed[k] = existing
					}
				}
				truncated++
			}
			applied++
		}
	}

	// Step 3: Purge expired tombstones (default 30 days)
	tombstoneRetention := 30 * 24 * time.Hour
	now := time.Now().UTC()
	var purged int
	for key, row := range indexed {
		if delAt, ok := row[btypes.ColDeletedAt]; ok && delAt != nil {
			if ts, ok := delAt.(string); ok {
				if t, err := time.Parse(time.RFC3339, ts); err == nil {
					if now.Sub(t) > tombstoneRetention {
						delete(indexed, key)
						purged++
					}
				}
			}
		}
	}

	// Step 4: Write new base
	newRows := make([]writer.Row, 0, len(indexed))
	for _, row := range indexed {
		newRows = append(newRows, row)
	}

	newBaseEpoch := table.LatestDeltaEpoch
	newBaseName := fmt.Sprintf("base_%06d.parquet", newBaseEpoch)
	remotePath := filepath.Join(table.Path, "base", newBaseName)
	tmpPath := filepath.Join("/tmp", "pgcdc-compact", remotePath)

	if err := writer.WriteBaseParquet(tmpPath, newRows, schema.Columns); err != nil {
		return fmt.Errorf("write new base: %w", err)
	}

	if err := uploadFile(ctx, sink, remotePath, tmpPath); err != nil {
		return fmt.Errorf("upload base: %w", err)
	}

	// Step 5: Update manifest
	t := m.Tables[tableName]
	t.BaseEpoch = newBaseEpoch
	t.RowCount = int64(len(indexed))
	m.Tables[tableName] = t

	// Step 6: Delete old delta files
	for _, deltaFile := range newDeltas {
		_ = sink.DeleteFile(ctx, deltaFile)
	}

	fmt.Printf("  %s: compacted %d deltas → base_%06d (%d rows, %d applied, %d truncate boundaries, %d purged)\n",
		tableName, len(newDeltas), newBaseEpoch, len(indexed), applied, truncated, purged)
	return nil
}

func readBase(ctx context.Context, sink ports.Sink, table manifest.Table) ([]writer.Row, error) {
	basePath := table.Path + "/base/"
	files, err := sink.ListFiles(ctx, basePath)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	return readParquetRows(ctx, sink, files[len(files)-1])
}

func readParquetRows(ctx context.Context, sink ports.Sink, path string) ([]writer.Row, error) {
	rc, err := sink.ReadFile(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	// Download to temp for parquet-go (needs seekable file)
	tmpFile, err := os.CreateTemp("", "pgcdc-read-*.parquet")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	if _, err := tmpFile.ReadFrom(rc); err != nil {
		return nil, err
	}
	if err := tmpFile.Close(); err != nil {
		return nil, err
	}

	f, err := os.Open(tmpFile.Name())
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("open parquet %s: %w", path, err)
	}

	schema := pf.Schema()
	fields := schema.Fields()
	colNames := make([]string, len(fields))
	for i, field := range fields {
		colNames[i] = field.Name()
	}

	var rows []writer.Row
	for _, rg := range pf.RowGroups() {
		rgRows := rg.NumRows()
		pqRows := make([]goparquet.Row, rgRows)
		for i := range pqRows {
			pqRows[i] = make(goparquet.Row, len(fields))
		}

		rgReader := rg.Rows()
		n, _ := rgReader.ReadRows(pqRows)
		_ = rgReader.Close()

		for i := 0; i < n; i++ {
			r := make(writer.Row, len(colNames))
			for j, colName := range colNames {
				if j < len(pqRows[i]) {
					r[colName] = parquetValueToGo(pqRows[i][j])
				}
			}
			rows = append(rows, r)
		}
	}

	return rows, nil
}

func parquetValueToGo(v goparquet.Value) any {
	if v.IsNull() {
		return nil
	}
	switch v.Kind() {
	case goparquet.Boolean:
		return v.Boolean()
	case goparquet.Int32:
		return int64(v.Int32())
	case goparquet.Int64:
		return v.Int64()
	case goparquet.Float:
		return float64(v.Float())
	case goparquet.Double:
		return v.Double()
	case goparquet.ByteArray:
		return string(v.ByteArray())
	default:
		return string(v.ByteArray())
	}
}

func pkKey(row writer.Row, pks []string) string {
	key := ""
	for _, pk := range pks {
		key += fmt.Sprintf("%v|", row[pk])
	}
	return key
}
