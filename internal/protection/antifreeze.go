package protection

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/exchanger"
)

type AntifreezeSession struct {
	ID        string    `json:"id"`
	StationID string    `json:"station_id"`
	Active    bool      `json:"active"`
	StartedAt time.Time `json:"started_at"`
	StoppedAt time.Time `json:"stopped_at,omitempty"`
}

type activeAntifreeze struct {
	session AntifreezeSession
	cancel  context.CancelFunc
	done    chan struct{}
}

type circulationRunner interface {
	Run(ctx context.Context, stationID string, started chan<- error)
	Running(stationID string) bool
}

type Antifreeze struct {
	runner       circulationRunner
	startTimeout time.Duration
	mu           sync.RWMutex
	active       map[string]*activeAntifreeze
}

func NewAntifreeze(runner *exchanger.CirculationRunner) *Antifreeze {
	return &Antifreeze{runner: runner, startTimeout: time.Second, active: make(map[string]*activeAntifreeze)}
}

func (a *Antifreeze) Start(requestContext context.Context, stationID string) (AntifreezeSession, error) {
	if err := requestContext.Err(); err != nil {
		return AntifreezeSession{}, err
	}
	a.mu.Lock()
	if current := a.active[stationID]; current != nil {
		a.mu.Unlock()
		return current.session, fmt.Errorf("antifreeze already active for %s", stationID)
	}
	session := AntifreezeSession{ID: uuid.NewString(), StationID: stationID, Active: true, StartedAt: time.Now().UTC()}
	ctx, cancel := context.WithCancel(context.Background())
	current := &activeAntifreeze{session: session, cancel: cancel, done: make(chan struct{})}
	a.active[stationID] = current
	a.mu.Unlock()

	// removeSelf only unregisters this attempt: a failed start must never delete
	// a newer session that has since taken over the station.
	removeSelf := func() {
		a.mu.Lock()
		if a.active[stationID] == current {
			delete(a.active, stationID)
		}
		a.mu.Unlock()
	}

	// Capacity one lets a late startup result land without blocking the worker
	// even after this function has already given up.
	started := make(chan error, 1)
	go func() {
		defer close(current.done)
		a.runner.Run(ctx, stationID, started)
	}()

	timer := time.NewTimer(a.startTimeout)
	defer timer.Stop()
	select {
	case err := <-started:
		if err != nil {
			cancel()
			removeSelf()
			return AntifreezeSession{}, fmt.Errorf("antifreeze circulation did not start: %w", err)
		}
		return session, nil
	case <-current.done:
		// If the worker signaled success (or failure) just before exiting, honor
		// that rather than reporting a spurious failure.
		select {
		case err := <-started:
			if err == nil {
				return session, nil
			}
			cancel()
			removeSelf()
			return AntifreezeSession{}, fmt.Errorf("antifreeze circulation did not start: %w", err)
		default:
		}
		// The worker exited without a startup result. Do not leave the dead
		// attempt occupying the station.
		cancel()
		removeSelf()
		return AntifreezeSession{}, fmt.Errorf("antifreeze circulation worker stopped unexpectedly")
	case <-timer.C:
		cancel()
		removeSelf()
		// Let the canceled worker fully release the pump before returning, so an
		// immediate retry is not rejected as a duplicate by the lower layer.
		select {
		case <-current.done:
		case <-time.After(a.startTimeout):
		}
		return AntifreezeSession{}, fmt.Errorf("antifreeze circulation did not start")
	}
}

func (a *Antifreeze) Stop(stationID string) (AntifreezeSession, error) {
	a.mu.Lock()
	current := a.active[stationID]
	if current == nil {
		a.mu.Unlock()
		return AntifreezeSession{}, fmt.Errorf("antifreeze not active for %s", stationID)
	}
	delete(a.active, stationID)
	current.cancel()
	a.mu.Unlock()
	select {
	case <-current.done:
	case <-time.After(time.Second):
		return AntifreezeSession{}, fmt.Errorf("antifreeze worker did not stop")
	}
	current.session.Active = false
	current.session.StoppedAt = time.Now().UTC()
	return current.session, nil
}

func (a *Antifreeze) Session(stationID string) (AntifreezeSession, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	current := a.active[stationID]
	if current == nil {
		return AntifreezeSession{}, false
	}
	return current.session, true
}

func (a *Antifreeze) Running(stationID string) bool {
	return a.runner.Running(stationID)
}

func (a *Antifreeze) Close() {
	a.mu.RLock()
	ids := make([]string, 0, len(a.active))
	for stationID := range a.active {
		ids = append(ids, stationID)
	}
	a.mu.RUnlock()
	for _, stationID := range ids {
		_, _ = a.Stop(stationID)
	}
}
