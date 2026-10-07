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

func (r *CirculationRunner) Run(ctx context.Context, stationID string, started chan<- error) {
	if err := r.pumps.Circulate(ctx, stationID, 5*time.Millisecond, started); err != nil && ctx.Err() == nil {
		r.mu.Lock()
		r.errors[stationID] = fmt.Errorf("antifreeze circulation: %w", err)
		r.mu.Unlock()
	}
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
