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

// signalStart reports a startup result to the waiter. Success is signalled by
// closing the channel (delivers nil like a ready signal); a failure sends the
// error. Callers must create started with capacity one so the send never blocks
// when the waiter has already given up.
func signalStart(started chan<- error, err error) {
	if started == nil {
		return
	}
	if err != nil {
		started <- err
		return
	}
	close(started)
}

func (r *Runner) Circulate(ctx context.Context, stationID string, interval time.Duration, started chan<- error) error {
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
		signalStart(started, err)
		return err
	}
	r.running[stationID] = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.running, stationID)
		r.mu.Unlock()
	}()
	// A closed channel delivers the zero value (nil) exactly like a success
	// signal; failures send the error first.
	signalStart(started, nil)
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
