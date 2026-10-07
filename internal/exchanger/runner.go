package exchanger

import (
	"context"
	"fmt"
	"sync"
	"time"

	"heatpulse/internal/pump"
)

type CirculationRunner struct {
	pumps  *pump.Runner
	mu     sync.RWMutex
	errors map[string]error
}

func NewCirculationRunner(pumps *pump.Runner) *CirculationRunner {
	return &CirculationRunner{pumps: pumps, errors: make(map[string]error)}
}

// Run drives a single circulation session. It forwards the pump's startup
// result (nil or an error) on ready exactly once.
func (r *CirculationRunner) Run(ctx context.Context, stationID string, ready chan<- error) {
	err := r.pumps.Circulate(ctx, stationID, 5*time.Millisecond, ready)
	r.mu.Lock()
	if err != nil && ctx.Err() == nil {
		r.errors[stationID] = fmt.Errorf("antifreeze circulation: %w", err)
	} else {
		delete(r.errors, stationID)
	}
	r.mu.Unlock()
}

func (r *CirculationRunner) Running(stationID string) bool {
	return r.pumps.Running(stationID)
}

func (r *CirculationRunner) Cycles(stationID string) int {
	return r.pumps.Ticks(stationID)
}

func (r *CirculationRunner) Error(stationID string) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.errors[stationID]
}
