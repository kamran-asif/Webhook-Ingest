package store_test

import (
	"context"
	"testing"

	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

func TestInsertEventThenExists(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10, Payload: []byte(`{}`),
	}

	exists, err := s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if exists {
		t.Fatal("expected event to be absent before insert")
	}

	if err := s.InsertEvent(ctx, evt); err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	exists, err = s.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected event to exist after insert")
	}
}

func TestIncrementAccountStatsAccumulates(t *testing.T) {
	s := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	if err := s.IncrementAccountStats(ctx, accountID, 30); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}
	if err := s.IncrementAccountStats(ctx, accountID, 12); err != nil {
		t.Fatalf("IncrementAccountStats: %v", err)
	}

	got, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("got %+v, want CallCount=2 TotalDurationSec=42", got)
	}
}

func TestUpsertCallThenMarkRecordingProcessed(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", Payload: []byte(`{}`),
	}
	if err := s.UpsertCall(ctx, evt); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}
	if err := s.MarkRecordingProcessed(ctx, callID); err != nil {
		t.Fatalf("MarkRecordingProcessed: %v", err)
	}

	var processed bool
	row := s.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true")
	}
}

func TestIngestEventTx_AtomicityAndIdempotency(t *testing.T) {
	s := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, s)
	ctx := context.Background()

	evt := store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 120, Payload: []byte(`{}`),
	}

	// First ingestion succeeds
	inserted, err := s.IngestEventTx(ctx, evt)
	if err != nil {
		t.Fatalf("IngestEventTx 1: %v", err)
	}
	if !inserted {
		t.Fatal("expected first ingestion to return inserted=true")
	}

	// Verify stats count = 1
	st, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats 1: %v", err)
	}
	if st.CallCount != 1 || st.TotalDurationSec != 120 {
		t.Fatalf("got %+v, want CallCount=1 TotalDurationSec=120", st)
	}

	// Second ingestion with same event_id must be ignored
	inserted2, err := s.IngestEventTx(ctx, evt)
	if err != nil {
		t.Fatalf("IngestEventTx 2: %v", err)
	}
	if inserted2 {
		t.Fatal("expected duplicate ingestion to return inserted=false")
	}

	// Verify stats remain count = 1 (no double counting)
	st2, err := s.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats 2: %v", err)
	}
	if st2.CallCount != 1 || st2.TotalDurationSec != 120 {
		t.Fatalf("got %+v, want CallCount=1 TotalDurationSec=120 after duplicate", st2)
	}
}
