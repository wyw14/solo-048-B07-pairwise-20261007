package pump

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Runner struct {
	mu      sync.RWMutex
	running map[string]bool
	ticks   map[string]int
}

func NewRunner() *Runner {
	return &Runner{running: make(map[string]bool), ticks: make(map[string]int)}
}

// Circulate runs the pump loop for a station. It registers the station before
// returning and reports startup on the ready channel: nil once circulation has
// started, or the error that prevented it. The channel is sent to at most once
// and is buffered, so a caller that has already given up never blocks the
// worker from exiting.
func (r *Runner) Circulate(ctx context.Context, stationID string, interval time.Duration, ready chan<- error) error {
	if stationID == "" {
		return fmt.Errorf("station id is required")
	}
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	r.mu.Lock()
	if r.running[stationID] {
		r.mu.Unlock()
		err := fmt.Errorf("station %s circulation already active", stationID)
		if ready != nil {
			ready <- err
		}
		return err
	}
	r.running[stationID] = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.running, stationID)
		r.mu.Unlock()
	}()
	if ready != nil {
		ready <- nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.mu.Lock()
			r.ticks[stationID]++
			r.mu.Unlock()
		}
	}
}

func (r *Runner) Running(stationID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.running[stationID]
}

func (r *Runner) Ticks(stationID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ticks[stationID]
}
