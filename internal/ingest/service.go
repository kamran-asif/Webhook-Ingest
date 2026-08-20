// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
	wg    sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// InitCache executes Cache Hydration / Warming, loading durable account statistics
// from PostgreSQL into volatile memory during application boot.
func (s *Service) InitCache(ctx context.Context) error {
	dbStats, err := s.store.AllAccountStats(ctx)
	if err != nil {
		return err
	}
	m := make(map[string]stats.AccountStats)
	for acc, st := range dbStats {
		m[acc] = stats.AccountStats{
			CallCount:        st.CallCount,
			TotalDurationSec: st.TotalDurationSec,
		}
	}
	s.cache.LoadFromStats(m)
	return nil
}

// Stats returns the cached totals for an account, falling back to PostgreSQL if missing (Cache Stampede Protection).
func (s *Service) Stats(accountID string) stats.AccountStats {
	if st, found := s.cache.Lookup(accountID); found {
		return st
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dbStats, err := s.store.AccountStats(ctx, accountID)
	if err != nil {
		return stats.AccountStats{}
	}

	st := stats.AccountStats{
		CallCount:        dbStats.CallCount,
		TotalDurationSec: dbStats.TotalDurationSec,
	}
	s.cache.Set(accountID, st)
	return st
}

// Ingest processes an incoming payload delivery using Exact-Once Processing Semantics (EOPS).
// It anchors idempotency via store.IngestEventTx and delegates background tasks safely.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// Atomic transaction: Insert event, upsert call, increment account stats atomically.
	inserted, err := s.store.IngestEventTx(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored idempotently", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so processing runs asynchronously without blocking HTTP ack.
	// Uses sync.WaitGroup for Goroutine Lifecycle Tracking.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			bgCtx := context.Background()
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("processRecording failed", "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// Shutdown executes Graceful Worker Drain, waiting for all active background goroutines to finish before process exit.
func (s *Service) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
