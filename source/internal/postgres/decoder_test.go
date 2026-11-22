package postgres

import (
	"encoding/binary"
	"testing"
	"time"
)

// putCString writes a NUL-terminated string into msg at off and returns the
// number of bytes written.
func putCString(msg []byte, off int, s string) int {
	copy(msg[off:], s)
	msg[off+len(s)] = 0
	return len(s) + 1
}

// relationV2 builds the pgoutput v2 wire bytes for a Relation message ('R')
// with one BIGINT primary-key column "id" and one TEXT column "name".
func relationV2(relationID uint32, namespace, name string) []byte {
	col1, col2 := "id", "name"
	col1Len := 1 + len(col1) + 1 + 4 + 4
	col2Len := 1 + len(col2) + 1 + 4 + 4

	msg := make([]byte, 1+4+len(namespace)+1+len(name)+1+1+2+col1Len+col2Len)
	off := 0
	msg[off] = 'R'
	off++
	binary.BigEndian.PutUint32(msg[off:], relationID)
	off += 4
	off += putCString(msg, off, namespace)
	off += putCString(msg, off, name)
	msg[off] = 1 // replica identity default
	off++
	binary.BigEndian.PutUint16(msg[off:], 2)
	off += 2

	msg[off] = 1 // part of replica identity
	off++
	off += putCString(msg, off, col1)
	binary.BigEndian.PutUint32(msg[off:], 20) // int8 OID
	off += 4
	binary.BigEndian.PutUint32(msg[off:], ^uint32(0))
	off += 4

	msg[off] = 0
	off++
	off += putCString(msg, off, col2)
	binary.BigEndian.PutUint32(msg[off:], 25) // text OID
	off += 4
	binary.BigEndian.PutUint32(msg[off:], ^uint32(0))
	off += 4

	return msg
}

// truncateV2 builds the wire bytes for a Truncate message ('T') covering the
// given relation IDs.
func truncateV2(relationIDs ...uint32) []byte {
	msg := make([]byte, 1+4+1+4*len(relationIDs))
	off := 0
	msg[off] = 'T'
	off++
	binary.BigEndian.PutUint32(msg[off:], uint32(len(relationIDs)))
	off += 4
	msg[off] = 0 // options: no cascade, no restart identity
	off++
	for _, id := range relationIDs {
		binary.BigEndian.PutUint32(msg[off:], id)
		off += 4
	}
	return msg
}

// TestDecode_Truncate verifies a TRUNCATE WAL message becomes a "T" event with
// no row payload, carrying the relation's qualified name and the WAL metadata.
func TestDecode_Truncate(t *testing.T) {
	d := newDecoder()
	ts := time.Date(2026, 11, 23, 9, 0, 0, 0, time.UTC)

	if _, err := d.decode(relationV2(42, "public", "staging_orders"), "0/16B3748", ts); err != nil {
		t.Fatalf("decode relation: %v", err)
	}

	evt, err := d.decode(truncateV2(42), "0/16B3750", ts)
	if err != nil {
		t.Fatalf("decode truncate: %v", err)
	}
	if evt == nil {
		t.Fatal("expected a truncate event, got nil")
	}
	if evt.Op != OpTruncate {
		t.Errorf("Op = %q, want %q", evt.Op, OpTruncate)
	}
	if evt.Table != "public.staging_orders" {
		t.Errorf("Table = %q, want public.staging_orders", evt.Table)
	}
	if evt.Row != nil {
		t.Errorf("Row = %v, want nil for truncate boundary", evt.Row)
	}
	if evt.LSN != "0/16B3750" {
		t.Errorf("LSN = %q, want 0/16B3750", evt.LSN)
	}
	if !evt.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", evt.Timestamp, ts)
	}
	if queued := d.DrainPending(); queued != nil {
		t.Errorf("expected no queued events for single-relation truncate, got %d", len(queued))
	}
}

// TestDecode_TruncateMultiRelation verifies one TRUNCATE statement over
// several relations fans out to one event per relation, with the first
// returned and the rest drained in WAL order via DrainPending.
func TestDecode_TruncateMultiRelation(t *testing.T) {
	d := newDecoder()
	ts := time.Now().UTC()

	if _, err := d.decode(relationV2(1, "public", "a"), "0/1", ts); err != nil {
		t.Fatalf("decode relation a: %v", err)
	}
	if _, err := d.decode(relationV2(2, "public", "b"), "0/2", ts); err != nil {
		t.Fatalf("decode relation b: %v", err)
	}
	// A third relation never sent a Relation message — its event must be skipped.
	first, err := d.decode(truncateV2(1, 2, 99), "0/3", ts)
	if err != nil {
		t.Fatalf("decode truncate: %v", err)
	}
	if first == nil || first.Table != "public.a" {
		t.Fatalf("first event = %v, want public.a", first)
	}

	queued := d.DrainPending()
	if len(queued) != 1 {
		t.Fatalf("queued events = %d, want 1", len(queued))
	}
	if queued[0].Table != "public.b" {
		t.Errorf("queued[0].Table = %q, want public.b", queued[0].Table)
	}
	if queued[0].Op != OpTruncate || queued[0].Row != nil {
		t.Errorf("queued[0] = %+v, want T event with nil row", queued[0])
	}
	if d.DrainPending() != nil {
		t.Error("DrainPending must reset after draining")
	}
}

// TestDecode_TruncateUnknownRelation ensures a TRUNCATE for a relation the
// decoder has not catalogued yet yields no event rather than a nil-pointer.
func TestDecode_TruncateUnknownRelation(t *testing.T) {
	d := newDecoder()
	evt, err := d.decode(truncateV2(7), "0/9", time.Now().UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if evt != nil {
		t.Errorf("expected nil event for unknown relation, got %+v", evt)
	}
}
