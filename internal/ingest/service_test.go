package ingest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string, duration int) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  %d,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, duration, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func getStats(t *testing.T, url, accountID string) (int64, int64) {
	t.Helper()
	resp, err := http.Get(url + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get stats status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var data struct {
		CallCount        int64 `json:"call_count"`
		TotalDurationSec int64 `json:"total_duration_sec"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatalf("unmarshal stats: %v", err)
	}
	return data.CallCount, data.TotalDurationSec
}

// 1. New webhook -> record created, stats +1
func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID, 143)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}

	callCount, totalDuration := getStats(t, srv.URL, accountID)
	if callCount != 1 || totalDuration != 143 {
		t.Fatalf("stats got count=%d duration=%d, want 1 and 143", callCount, totalDuration)
	}
}

// 2. Same webhook twice -> only 1 record, stats +1
func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID, 143)
	for i := 0; i < 2; i++ {
		resp := post(t, srv.URL+"/webhooks/calls", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}

	callCount, totalDuration := getStats(t, srv.URL, accountID)
	if callCount != 1 || totalDuration != 143 {
		t.Fatalf("stats got count=%d duration=%d, want 1 and 143", callCount, totalDuration)
	}
}

// 3. Same webhook 10 times -> only 1 record, stats +1
func TestSameWebhook10Times_OnlyOneRecordAndStatsPlusOne(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID, 200)
	for i := 0; i < 10; i++ {
		resp := post(t, srv.URL+"/webhooks/calls", body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var count int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("scan events count: %v", err)
	}
	if count != 1 {
		t.Fatalf("events count=%d, want 1", count)
	}

	callCount, totalDuration := getStats(t, srv.URL, accountID)
	if callCount != 1 || totalDuration != 200 {
		t.Fatalf("stats got count=%d duration=%d, want 1 and 200", callCount, totalDuration)
	}
}

// 4. Duplicate after HTTP 200 -> still ignored
func TestDuplicateAfterHTTP200_StillIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := eventJSON(eventID, callID, accountID, 50)
	r1 := post(t, srv.URL+"/webhooks/calls", body)
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("first delivery got %d, want 200", r1.StatusCode)
	}

	// Later redelivery
	r2 := post(t, srv.URL+"/webhooks/calls", body)
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("redelivery got %d, want 200", r2.StatusCode)
	}

	callCount, totalDuration := getStats(t, srv.URL, accountID)
	if callCount != 1 || totalDuration != 50 {
		t.Fatalf("stats after redelivery: got count=%d duration=%d, want 1 and 50", callCount, totalDuration)
	}
}

// 5. Concurrent duplicate requests -> exactly 1 record, stats +1
func TestConcurrentDuplicateRequests_ExactlyOneRecordAndStatsPlusOne(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID, 100)
	const concurrency = 15

	var wg sync.WaitGroup
	wg.Add(concurrency)
	statusCodes := make([]int, concurrency)

	for i := 0; i < concurrency; i++ {
		idx := i
		go func() {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				statusCodes[idx] = 0
				return
			}
			statusCodes[idx] = resp.StatusCode
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()

	for i, code := range statusCodes {
		if code != http.StatusOK {
			t.Fatalf("goroutine %d got status %d, want 200", i, code)
		}
	}

	var count int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&count); err != nil {
		t.Fatalf("scan events count: %v", err)
	}
	if count != 1 {
		t.Fatalf("concurrent events count=%d, want 1", count)
	}

	callCount, totalDuration := getStats(t, srv.URL, accountID)
	if callCount != 1 || totalDuration != 100 {
		t.Fatalf("concurrent stats got count=%d duration=%d, want 1 and 100", callCount, totalDuration)
	}
}

// 6. Different event_ids -> each processed independently
func TestDifferentEventIDs_ProcessedIndependently(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)

	eventID1 := "evt_indep_1_" + accountID
	callID1 := "call_indep_1_" + accountID
	eventID2 := "evt_indep_2_" + accountID
	callID2 := "call_indep_2_" + accountID

	body1 := eventJSON(eventID1, callID1, accountID, 60)
	body2 := eventJSON(eventID2, callID2, accountID, 40)

	if resp := post(t, srv.URL+"/webhooks/calls", body1); resp.StatusCode != http.StatusOK {
		t.Fatalf("body1 got %d, want 200", resp.StatusCode)
	}
	if resp := post(t, srv.URL+"/webhooks/calls", body2); resp.StatusCode != http.StatusOK {
		t.Fatalf("body2 got %d, want 200", resp.StatusCode)
	}

	callCount, totalDuration := getStats(t, srv.URL, accountID)
	if callCount != 2 || totalDuration != 100 {
		t.Fatalf("independent stats got count=%d duration=%d, want 2 and 100", callCount, totalDuration)
	}
}

// 8. Invalid webhook -> rejected, nothing persisted
func TestInvalidWebhook_RejectedAndNothingPersisted(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// Invalid status "unknown_status"
	invalidBody := fmt.Sprintf(`{
	  "event_id": %q,
	  "call_id": %q,
	  "account_id": %q,
	  "status": "unknown_status",
	  "duration_sec": 50
	}`, eventID, callID, accountID)

	resp := post(t, srv.URL+"/webhooks/calls", invalidBody)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid webhook got %d, want 400", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if exists {
		t.Fatal("expected event not to be persisted for invalid webhook")
	}

	callCount, totalDuration := getStats(t, srv.URL, accountID)
	if callCount != 0 || totalDuration != 0 {
		t.Fatalf("stats got count=%d duration=%d, want 0 and 0", callCount, totalDuration)
	}
}
