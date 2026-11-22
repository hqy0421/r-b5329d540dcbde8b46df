package internal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

const testTable = "public.staging_detail"

var testCols = []manifest.Column{
	{Name: "id", PgType: "bigint", ParquetType: "INT64", Nullable: false},
	{Name: "label", PgType: "text", ParquetType: "UTF8", Nullable: true},
}

func newTestSink(t *testing.T) (context.Context, *filesystem.Adapter, string) {
	t.Helper()
	dir := t.TempDir()
	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("filesystem sink: %v", err)
	}
	return context.Background(), sink, dir
}

// seedManifest writes a manifest describing one active, PK-keyed table with a
// base snapshot at epoch 0.
func seedManifest(ctx context.Context, t *testing.T, sink *filesystem.Adapter, baseRows []writer.Row) {
	t.Helper()
	basePath := filepath.Join(testTable, "base", "base_000000.parquet")
	tmp := filepath.Join(t.TempDir(), "base_000000.parquet")
	if err := writer.WriteBaseParquet(tmp, baseRows, testCols); err != nil {
		t.Fatalf("write base: %v", err)
	}
	if err := uploadFile(ctx, sink, basePath, tmp); err != nil {
		t.Fatalf("upload base: %v", err)
	}

	m := &manifest.Manifest{
		Version: 1,
		Source:  manifest.Source{Name: "slot", InitLSN: "0/0", InitTimestamp: time.Now().UTC()},
		Tables: map[string]manifest.Table{
			testTable: {
				Status: "active", PrimaryKey: []string{"id"}, SchemaVersion: 1,
				BaseEpoch: 0, LatestDeltaEpoch: 0, RowCount: int64(len(baseRows)),
				Path: testTable,
			},
		},
		Schemas: map[string]manifest.Schema{
			testTable: {Version: 1, Columns: testCols},
		},
		UpdatedAt: time.Now().UTC(),
	}
	if err := sink.WriteManifest(ctx, m); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

// writeDelta uploads one epoch file of CDC rows and bumps the manifest's
// LatestDeltaEpoch to match.
func writeDelta(ctx context.Context, t *testing.T, sink *filesystem.Adapter, n int64, rows []writer.Row) {
	t.Helper()
	name := epoch.FormatFilename(n)
	remote := filepath.Join(testTable, "deltas", name)
	tmp := filepath.Join(t.TempDir(), name)
	if err := writer.WriteDeltaParquet(tmp, rows, testCols, n); err != nil {
		t.Fatalf("write delta %d: %v", n, err)
	}
	if err := uploadFile(ctx, sink, remote, tmp); err != nil {
		t.Fatalf("upload delta %d: %v", n, err)
	}

	m, err := sink.ReadManifest(ctx)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	tbl := m.Tables[testTable]
	tbl.LatestDeltaEpoch = n
	m.Tables[testTable] = tbl
	if err := sink.WriteManifest(ctx, m); err != nil {
		t.Fatalf("bump manifest: %v", err)
	}
}

func truncateRow() writer.Row { return writer.Row{btypes.ColOp: "T"} }

func insertRow(id int64, label string) writer.Row {
	return writer.Row{btypes.ColOp: "I", "id": id, "label": label}
}

// readLatestBase loads the highest-numbered base file for the table.
func readLatestBase(ctx context.Context, t *testing.T, sink *filesystem.Adapter) []writer.Row {
	t.Helper()
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	tbl := m.Tables[testTable]
	files, err := sink.ListFiles(ctx, filepath.Join(tbl.Path, "base"))
	if err != nil {
		t.Fatalf("list base: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no base files")
	}
	rows, err := readParquetRows(ctx, sink, files[len(files)-1])
	if err != nil {
		t.Fatalf("read base rows: %v", err)
	}
	return rows
}

// liveRows returns base rows that carry no tombstone.
func liveRows(rows []writer.Row) map[int64]writer.Row {
	out := map[int64]writer.Row{}
	for _, r := range rows {
		if d, ok := r[btypes.ColDeletedAt]; ok && d != nil {
			continue
		}
		out[r["id"].(int64)] = r
	}
	return out
}

// TestCompact_TruncateTombstonesAllAndReinsertSurvives is the daily reload
// scenario: TRUNCATE then re-import. After compaction every pre-truncation row
// is a tombstone, while rows inserted after the truncation boundary are live.
func TestCompact_TruncateTombstonesAllAndReinsertSurvives(t *testing.T) {
	ctx, sink, _ := newTestSink(t)
	seedManifest(ctx, t, sink, []writer.Row{
		{"id": int64(1), "label": "yesterday-1"},
		{"id": int64(2), "label": "yesterday-2"},
	})

	// Epoch 1: TRUNCATE the table, then the daily job re-inserts rows.
	writeDelta(ctx, t, sink, 1, []writer.Row{
		truncateRow(),
		insertRow(1, "today-1"),
		insertRow(3, "today-3"),
	})

	if err := CompactTables(ctx, sink, []string{testTable}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	rows := readLatestBase(ctx, t, sink)
	if len(rows) != 3 {
		t.Fatalf("base rows = %d, want 3 (2 truncated tombstones + re-inserted)", len(rows))
	}

	live := liveRows(rows)
	if len(live) != 2 {
		t.Fatalf("live rows = %d, want 2", len(live))
	}
	if live[1]["label"] != "today-1" {
		t.Errorf("re-inserted id=1 label = %v, want today-1", live[1]["label"])
	}
	if _, ok := live[2]; ok {
		t.Error("id=2 truncated yesterday but still live")
	}
	if live[3]["label"] != "today-3" {
		t.Errorf("id=3 label = %v, want today-3", live[3]["label"])
	}

	m, _ := sink.ReadManifest(ctx)
	tbl := m.Tables[testTable]
	if tbl.BaseEpoch != 1 {
		t.Errorf("BaseEpoch = %d, want 1", tbl.BaseEpoch)
	}
	if tbl.RowCount != 3 {
		t.Errorf("RowCount = %d, want 3 (tombstones retained until TTL)", tbl.RowCount)
	}

	// The truncation delta must have been consumed and deleted.
	deltas, err := sink.ListFiles(ctx, filepath.Join(testTable, "deltas"))
	if err != nil {
		t.Fatalf("list deltas: %v", err)
	}
	if len(deltas) != 0 {
		t.Errorf("deltas after compact = %v, want empty", deltas)
	}
}

// TestCompact_TruncateBoundaryOnly produces an all-tombstone base when no rows
// are re-inserted after the truncation, so the current view is empty.
func TestCompact_TruncateBoundaryOnly(t *testing.T) {
	ctx, sink, _ := newTestSink(t)
	seedManifest(ctx, t, sink, []writer.Row{
		{"id": int64(1), "label": "a"},
		{"id": int64(2), "label": "b"},
	})
	writeDelta(ctx, t, sink, 1, []writer.Row{truncateRow()})

	if err := CompactTables(ctx, sink, []string{testTable}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	rows := readLatestBase(ctx, t, sink)
	if len(rows) != 2 {
		t.Fatalf("base rows = %d, want 2 tombstones", len(rows))
	}
	if live := liveRows(rows); len(live) != 0 {
		t.Errorf("live rows = %d, want 0 after truncate-only", len(live))
	}
	for _, r := range rows {
		if r[btypes.ColDeletedAt] == nil {
			t.Errorf("row %v missing __deleted_at", r)
		}
	}
}

// TestCompact_OldBaseAndManifestRemainReadable verifies time travel:
// compaction writes a NEW base file but never removes the pre-truncation base,
// and a copy of the old manifest still resolves that historical snapshot.
func TestCompact_OldBaseAndManifestRemainReadable(t *testing.T) {
	ctx, sink, dir := newTestSink(t)
	seedManifest(ctx, t, sink, []writer.Row{
		{"id": int64(1), "label": "yesterday-1"},
		{"id": int64(2), "label": "yesterday-2"},
	})

	// Snapshot the manifest as it was before the truncation.
	oldManifestBytes, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("read old manifest: %v", err)
	}

	writeDelta(ctx, t, sink, 1, []writer.Row{
		truncateRow(),
		insertRow(1, "today-1"),
	})
	if err := CompactTables(ctx, sink, []string{testTable}); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Both base files must still exist on disk.
	baseFiles, err := sink.ListFiles(ctx, filepath.Join(testTable, "base"))
	if err != nil {
		t.Fatalf("list base: %v", err)
	}
	if len(baseFiles) != 2 {
		t.Fatalf("base files = %v, want both epoch-0 and epoch-1 bases retained", baseFiles)
	}

	// Reconstruct the OLD manifest in an isolated root and read the historical
	// base it points at (base_000000.parquet), proving the pre-truncation view
	// survives and still shows yesterday's rows.
	histRoot := t.TempDir()
	histSink, err := filesystem.New(histRoot)
	if err != nil {
		t.Fatalf("hist sink: %v", err)
	}
	oldBase := filepath.Join(testTable, "base", "base_000000.parquet")
	if err := uploadFile(ctx, histSink, oldBase, filepath.Join(dir, oldBase)); err != nil {
		t.Fatalf("copy old base: %v", err)
	}
	if err := os.WriteFile(filepath.Join(histRoot, "manifest.json"), oldManifestBytes, 0644); err != nil {
		t.Fatalf("write old manifest: %v", err)
	}

	histRows, err := readParquetRows(ctx, histSink, oldBase)
	if err != nil {
		t.Fatalf("read historical base: %v", err)
	}
	histLive := liveRows(histRows)
	if len(histLive) != 2 {
		t.Fatalf("historical live rows = %d, want 2", len(histLive))
	}
	if histLive[1]["label"] != "yesterday-1" || histLive[2]["label"] != "yesterday-2" {
		t.Errorf("historical data changed: %v", histLive)
	}
}

// TestCompact_TruncateTombstonesPurgedAfterTTL verifies truncation tombstones
// are hard-purged once they outlive the 30-day retention window on a later
// compaction cycle. The purge operates on the CURRENT base; old base files
// remain on disk for time travel regardless.
func TestCompact_TruncateTombstonesPurgedAfterTTL(t *testing.T) {
	ctx, sink, _ := newTestSink(t)
	seedManifest(ctx, t, sink, []writer.Row{
		{"id": int64(1), "label": "a"},
		{"id": int64(2), "label": "b"},
	})
	writeDelta(ctx, t, sink, 1, []writer.Row{truncateRow()})
	if err := CompactTables(ctx, sink, nil); err != nil {
		t.Fatalf("first compact: %v", err)
	}

	// Backdate every tombstone in the current base beyond the retention
	// window by rewriting the base file directly.
	m, _ := sink.ReadManifest(ctx)
	tbl := m.Tables[testTable]
	baseFiles, err := sink.ListFiles(ctx, filepath.Join(tbl.Path, "base"))
	if err != nil {
		t.Fatalf("list base: %v", err)
	}
	currentBase := baseFiles[len(baseFiles)-1]
	rows, err := readParquetRows(ctx, sink, currentBase)
	if err != nil {
		t.Fatalf("read base: %v", err)
	}
	old := time.Now().UTC().Add(-31 * 24 * time.Hour).Format(time.RFC3339)
	for _, r := range rows {
		r[btypes.ColDeletedAt] = old
	}
	tmp := filepath.Join(t.TempDir(), "aged.parquet")
	if err := writer.WriteBaseParquet(tmp, rows, testCols); err != nil {
		t.Fatalf("rewrite aged base: %v", err)
	}
	if err := uploadFile(ctx, sink, currentBase, tmp); err != nil {
		t.Fatalf("overwrite aged base: %v", err)
	}

	// A no-op delta is needed so LatestDeltaEpoch > BaseEpoch and compaction
	// re-processes the table; the insert adds one fresh live row.
	writeDelta(ctx, t, sink, 2, []writer.Row{insertRow(9, "fresh")})
	if err := CompactTables(ctx, sink, nil); err != nil {
		t.Fatalf("second compact: %v", err)
	}

	final := readLatestBase(ctx, t, sink)
	if len(final) != 1 {
		t.Fatalf("rows after TTL purge = %d, want 1 (aged tombstones gone, fresh row kept)", len(final))
	}
	live := liveRows(final)
	if live[9]["label"] != "fresh" {
		t.Errorf("fresh row missing after purge: %v", live)
	}

	// Historical base_000000 (pre-truncation) is still on disk.
	baseFiles2, _ := sink.ListFiles(ctx, filepath.Join(tbl.Path, "base"))
	foundEpoch0 := false
	for _, f := range baseFiles2 {
		if filepath.Base(f) == "base_000000.parquet" {
			foundEpoch0 = true
		}
	}
	if !foundEpoch0 {
		t.Error("pre-truncation base_000000.parquet was removed — time travel broken")
	}
}

// TestCompact_TruncateThenReinsertSamePKAcrossEpochs verifies a re-inserted PK
// is genuinely resurrected — its stale truncation tombstone must not survive
// into the new base, or the 30-day TTL would later purge live data.
func TestCompact_TruncateThenReinsertSamePKAcrossEpochs(t *testing.T) {
	ctx, sink, _ := newTestSink(t)
	seedManifest(ctx, t, sink, []writer.Row{
		{"id": int64(7), "label": "old"},
	})
	// Epoch 1: truncate. Epoch 2: same PK comes back in the fresh import.
	writeDelta(ctx, t, sink, 1, []writer.Row{truncateRow()})
	writeDelta(ctx, t, sink, 2, []writer.Row{insertRow(7, "new")})

	if err := CompactTables(ctx, sink, nil); err != nil { // nil = compact all active
		t.Fatalf("compact: %v", err)
	}

	rows := readLatestBase(ctx, t, sink)
	live := liveRows(rows)
	if len(live) != 1 {
		t.Fatalf("live rows = %d, want 1 resurrected row", len(live))
	}
	if live[7]["label"] != "new" {
		t.Errorf("id=7 label = %v, want new", live[7]["label"])
	}
}
