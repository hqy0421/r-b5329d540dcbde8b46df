package postgres

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

// OpTruncate is the CDC operation code for a TRUNCATE boundary. Unlike
// I/U/D it carries no row data: the whole relation was emptied at this LSN.
const OpTruncate = "T"

// decoder translates pgoutput v2 WAL messages into CDCEvents.
type decoder struct {
	relations map[uint32]*pglogrepl.RelationMessageV2
	// pending holds events produced by one WAL message that fans out to
	// multiple relations (today: multi-relation TRUNCATE). DrainPending
	// must be consumed before decoding the next WAL message.
	pending []*CDCEvent
}

func newDecoder() *decoder {
	return &decoder{relations: make(map[uint32]*pglogrepl.RelationMessageV2)}
}

// DrainPending returns boundary events buffered by the previous decode call
// (e.g. the 2nd..Nth relations of a multi-table TRUNCATE). The stream loop
// forwards these before reading the next WAL message so event order matches
// the WAL order.
func (d *decoder) DrainPending() []*CDCEvent {
	if len(d.pending) == 0 {
		return nil
	}
	out := d.pending
	d.pending = nil
	return out
}

func (d *decoder) decode(walData []byte, lsn string, ts time.Time) (*CDCEvent, error) {
	msg, err := pglogrepl.ParseV2(walData, false)
	if err != nil {
		return nil, fmt.Errorf("parse v2: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		d.relations[m.RelationID] = m
		return nil, nil

	case *pglogrepl.InsertMessageV2:
		rel := d.relations[m.RelationID]
		if rel == nil {
			return nil, nil
		}
		row := decodeTuple(m.Tuple, rel)
		return &CDCEvent{
			Table:     fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName),
			Op:        "I",
			Row:       row,
			LSN:       lsn,
			Timestamp: ts,
		}, nil

	case *pglogrepl.UpdateMessageV2:
		rel := d.relations[m.RelationID]
		if rel == nil {
			return nil, nil
		}
		row := decodeTuple(m.NewTuple, rel)
		return &CDCEvent{
			Table:     fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName),
			Op:        "U",
			Row:       row,
			LSN:       lsn,
			Timestamp: ts,
		}, nil

	case *pglogrepl.DeleteMessageV2:
		rel := d.relations[m.RelationID]
		if rel == nil {
			return nil, nil
		}
		row := decodeTuple(m.OldTuple, rel)
		return &CDCEvent{
			Table:     fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName),
			Op:        "D",
			Row:       row,
			LSN:       lsn,
			Timestamp: ts,
		}, nil

	case *pglogrepl.BeginMessage:
		return nil, nil
	case *pglogrepl.CommitMessage:
		return nil, nil
	case *pglogrepl.TypeMessageV2:
		return nil, nil
	case *pglogrepl.OriginMessage:
		return nil, nil
	case *pglogrepl.TruncateMessageV2:
		// A single TRUNCATE WAL message can cover several relations
		// (TRUNCATE a, b / CASCADE). Emit one boundary event per relation
		// the decoder has already seen. Relations are resolved via the
		// RelationMessageV2 catalog, exactly like I/U/D.
		var events []*CDCEvent
		for _, relID := range m.RelationIDs {
			rel := d.relations[relID]
			if rel == nil {
				continue
			}
			events = append(events, &CDCEvent{
				Table:     fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName),
				Op:        OpTruncate,
				Row:       nil,
				LSN:       lsn,
				Timestamp: ts,
			})
		}
		if len(events) == 0 {
			return nil, nil
		}
		// decode returns a single event; callers expecting one event per WAL
		// message are served the first relation, and any remainder is handed
		// back through the decoder's queued channel.
		d.pending = append(d.pending, events[1:]...)
		return events[0], nil
	case *pglogrepl.StreamStartMessageV2:
		return nil, nil
	case *pglogrepl.StreamStopMessageV2:
		return nil, nil
	case *pglogrepl.StreamCommitMessageV2:
		return nil, nil
	case *pglogrepl.StreamAbortMessageV2:
		return nil, nil
	default:
		return nil, nil
	}
}

func decodeTuple(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) map[string]any {
	if tuple == nil {
		return nil
	}
	row := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			row[colName] = nil
		case 't': // text
			row[colName] = string(col.Data)
		case 'u': // unchanged toast
			row[colName] = nil
		default:
			row[colName] = string(col.Data)
		}
	}
	return row
}
