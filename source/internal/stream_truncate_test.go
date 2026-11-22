package internal

import (
	"context"
	"testing"
	"time"

	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// stubSource satisfies ports.Source for flush tests. Only GetTableSchema is
// reachable on this path (schema evolution); truncate events never invoke it,
// so the stub returns an error that tests should never trip.
type stubSource struct{}

func (stubSource) ListTables(context.Context, string) ([]string, error) { return nil, nil }
func (stubSource) GetTableSchema(context.Context, string, string) ([]ports.ColumnInfo, error) {
	return nil, nil
}
func (stubSource) GetPrimaryKeys(context.Context, string, string) ([]string, error) {
	return nil, nil
}
func (stubSource) Version(context.Context) (string, error) { return "", nil }
func (stubSource) CreateReplicationSlot(context.Context, string) (ports.Snapshot, error) {
	return ports.Snapshot{}, nil
}
func (stubSource) ReleaseSnapshot(context.Context)                           {}
func (stubSource) CreatePublication(context.Context, string, []string) error { return nil }
func (stubSource) CopyTable(context.Context, string, string, string) ([]map[string]any, []ports.ColumnInfo, error) {
	return nil, nil, nil
}
func (stubSource) Stream(context.Context, string, string, string, chan<- ports.CDCEvent) error {
	return nil
}
func (stubSource) BuildRoleProfiles(context.Context, []string) (map[string]ports.RoleProfile, error) {
	return nil, nil
}
func (stubSource) DropReplicationSlot(context.Context, string) error  { return nil }
func (stubSource) DropPublication(context.Context, string) error      { return nil }
func (stubSource) QueryRow(context.Context, string, ...any) ports.Row { return nil }
func (stubSource) Close()                                             {}

// TestFlush_TruncateWritesBoundaryDelta drives a TRUNCATE event through the
// real flush path and asserts: (1) a delta Parquet row with __op="T" is
// written, (2) the table is queued for compaction, (3) a normal event in the
// same flush is not mistaken for a truncation.
func TestFlush_TruncateWritesBoundaryDelta(t *testing.T) {
	ctx, sink, _ := newTestSink(t)
	seedManifest(ctx, t, sink, []writer.Row{{"id": int64(1), "label": "seed"}})

	m, err := sink.ReadManifest(ctx)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	buf := &eventBuffer{
		source:   stubSource{},
		sink:     sink,
		manifest: m,
		tables:   m.ActiveTables(),
	}

	now := time.Now().UTC()
	buf.add(ports.CDCEvent{Table: testTable, Op: "T", Row: nil, LSN: "0/A", Timestamp: now})
	buf.add(ports.CDCEvent{Table: testTable, Op: "I", Row: map[string]any{"id": int64(9), "label": "fresh"}, LSN: "0/B", Timestamp: now})

	if err := buf.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The truncation table must be queued for immediate compaction.
	queued := buf.takeTruncated()
	if len(queued) != 1 || queued[0] != testTable {
		t.Fatalf("truncated tables = %v, want [%s]", queued, testTable)
	}

	// A delta file exists and its T row round-trips through Parquet.
	m, _ = sink.ReadManifest(ctx)
	tbl := m.Tables[testTable]
	if tbl.LatestDeltaEpoch != 1 {
		t.Fatalf("LatestDeltaEpoch = %d, want 1", tbl.LatestDeltaEpoch)
	}
	deltaPath := testTable + "/deltas/epoch=000001.parquet"
	rows, err := readParquetRows(ctx, sink, deltaPath)
	if err != nil {
		t.Fatalf("read delta: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("delta rows = %d, want 2", len(rows))
	}
	var sawT, sawI bool
	for _, r := range rows {
		switch r[btypes.ColOp] {
		case "T":
			sawT = true
			// id is NOT NULL INT64, so a missing value reads back as the
			// parquet zero value (0). Assert on the nullable text column,
			// which round-trips an actual NULL, to prove no row payload.
			if r["label"] != nil {
				t.Errorf("truncate row should have NULL label, got %v", r["label"])
			}
		case "I":
			sawI = true
			if r["label"] != "fresh" {
				t.Errorf("insert label = %v, want fresh", r["label"])
			}
		}
	}
	if !sawT || !sawI {
		t.Errorf("delta missing ops: T=%v I=%v", sawT, sawI)
	}
}

// TestFlushThenCompact_DoesNotRegressBaseEpoch is the regression test for the
// in-process compact path: after a truncation flush compacts (advancing
// BaseEpoch in the sink manifest), the following ordinary flush must not write
// back the buffer's stale pre-compaction manifest and regress BaseEpoch.
func TestFlushThenCompact_DoesNotRegressBaseEpoch(t *testing.T) {
	ctx, sink, _ := newTestSink(t)
	seedManifest(ctx, t, sink, []writer.Row{{"id": int64(1), "label": "seed"}})

	m, _ := sink.ReadManifest(ctx)
	buf := &eventBuffer{
		source:   stubSource{},
		sink:     sink,
		manifest: m,
		tables:   m.ActiveTables(),
	}

	// Flush 1: truncate boundary → triggers an inline compaction to epoch 1.
	buf.add(ports.CDCEvent{Table: testTable, Op: "T", Row: nil})
	if err := buf.flushThenCompact(ctx); err != nil {
		t.Fatalf("flushThenCompact (truncate): %v", err)
	}
	afterCompact, _ := sink.ReadManifest(ctx)
	if got := afterCompact.Tables[testTable].BaseEpoch; got != 1 {
		t.Fatalf("BaseEpoch after compact = %d, want 1", got)
	}
	if buf.manifest.Tables[testTable].BaseEpoch != 1 {
		t.Fatal("buffer manifest was not refreshed after compact")
	}

	// Flush 2: an ordinary insert. Its manifest write must preserve BaseEpoch 1.
	buf.add(ports.CDCEvent{Table: testTable, Op: "I", Row: map[string]any{"id": int64(5), "label": "new"}})
	if err := buf.flushThenCompact(ctx); err != nil {
		t.Fatalf("flushThenCompact (insert): %v", err)
	}
	final, _ := sink.ReadManifest(ctx)
	tbl := final.Tables[testTable]
	if tbl.BaseEpoch != 1 {
		t.Errorf("BaseEpoch regressed to %d after a later flush, want 1", tbl.BaseEpoch)
	}
	if tbl.LatestDeltaEpoch != 2 {
		t.Errorf("LatestDeltaEpoch = %d, want 2", tbl.LatestDeltaEpoch)
	}

	// The epoch-1 truncation delta was consumed by the compact and must not
	// reappear; only the epoch-2 delta remains.
	deltas, err := sink.ListFiles(ctx, testTable+"/deltas")
	if err != nil {
		t.Fatalf("list deltas: %v", err)
	}
	if len(deltas) != 1 || deltas[0] != testTable+"/deltas/epoch=000002.parquet" {
		t.Errorf("remaining deltas = %v, want only epoch=000002.parquet", deltas)
	}
}

// TestFlush_NoTruncateLeavesCompactionQueueEmpty proves ordinary I/U/D flushes
// do not trigger a compaction pass.
func TestFlush_NoTruncateLeavesCompactionQueueEmpty(t *testing.T) {
	ctx, sink, _ := newTestSink(t)
	seedManifest(ctx, t, sink, []writer.Row{{"id": int64(1), "label": "seed"}})

	m, _ := sink.ReadManifest(ctx)
	buf := &eventBuffer{
		source:   stubSource{},
		sink:     sink,
		manifest: m,
		tables:   m.ActiveTables(),
	}
	buf.add(ports.CDCEvent{Table: testTable, Op: "I", Row: map[string]any{"id": int64(2), "label": "x"}})

	if err := buf.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if q := buf.takeTruncated(); len(q) != 0 {
		t.Errorf("expected empty compaction queue for non-truncate flush, got %v", q)
	}
}
