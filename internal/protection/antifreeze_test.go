package protection

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeRun struct {
	// run is invoked with the attempt's context and startup channel.
	run func(ctx context.Context, started chan<- error)
}

type fakeRunner struct {
	mu      sync.Mutex
	runs    []fakeRun
	running bool
}

func (f *fakeRunner) Run(ctx context.Context, _ string, started chan<- error) {
	f.mu.Lock()
	var next fakeRun
	if len(f.runs) > 0 {
		next = f.runs[0]
		f.runs = f.runs[1:]
	}
	f.running = true
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.running = false
		f.mu.Unlock()
	}()
	if next.run != nil {
		next.run(ctx, started)
	}
}

func (f *fakeRunner) Running(_ string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.running
}

func (f *fakeRunner) enqueue(run func(ctx context.Context, started chan<- error)) {
	f.mu.Lock()
	f.runs = append(f.runs, fakeRun{run: run})
	f.mu.Unlock()
}

func newTestAntifreeze(runner *fakeRunner) *Antifreeze {
	return &Antifreeze{
		runner:       runner,
		startTimeout: 25 * time.Millisecond,
		active:       make(map[string]*activeAntifreeze),
	}
}

var hangUntilCanceled = func(ctx context.Context, started chan<- error) {
	<-ctx.Done()
}

var succeedThenRun = func(ctx context.Context, started chan<- error) {
	close(started)
	<-ctx.Done()
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

func TestStartTimeoutReleasesStation(t *testing.T) {
	runner := &fakeRunner{}
	af := newTestAntifreeze(runner)
	runner.enqueue(hangUntilCanceled)

	if _, err := af.Start(context.Background(), "st-1"); err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if _, ok := af.Session("st-1"); ok {
		t.Fatal("timed-out start must not occupy the station")
	}

	// A subsequent start must work instead of being rejected as already active.
	runner.enqueue(succeedThenRun)
	session, err := af.Start(context.Background(), "st-1")
	if err != nil {
		t.Fatalf("retry after timeout failed: %v", err)
	}
	if !session.Active {
		t.Fatal("retry session should be active")
	}
	if _, err := af.Stop("st-1"); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestStartWorkerErrorReleasesStation(t *testing.T) {
	runner := &fakeRunner{}
	af := newTestAntifreeze(runner)
	boom := errors.New("pump offline")
	runner.enqueue(func(_ context.Context, started chan<- error) {
		started <- boom
	})

	_, err := af.Start(context.Background(), "st-2")
	if err == nil {
		t.Fatal("expected startup error")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped worker error %v, got %v", boom, err)
	}
	if _, ok := af.Session("st-2"); ok {
		t.Fatal("failed start must not occupy the station")
	}

	runner.enqueue(succeedThenRun)
	if _, err := af.Start(context.Background(), "st-2"); err != nil {
		t.Fatalf("retry after failed start should succeed, got %v", err)
	}
	if _, err := af.Stop("st-2"); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestWorkerExitsUnexpectedlyReleasesStation(t *testing.T) {
	runner := &fakeRunner{}
	af := newTestAntifreeze(runner)
	runner.enqueue(func(_ context.Context, _ chan<- error) {}) // returns immediately, no signal

	if _, err := af.Start(context.Background(), "st-3"); err == nil {
		t.Fatal("expected error when worker exits without signaling")
	}
	if _, ok := af.Session("st-3"); ok {
		t.Fatal("dead worker must not occupy the station")
	}
}

func TestOnlyOneActiveSessionPerStation(t *testing.T) {
	runner := &fakeRunner{}
	af := newTestAntifreeze(runner)
	runner.enqueue(succeedThenRun)
	runner.enqueue(succeedThenRun) // must never run

	first, err := af.Start(context.Background(), "st-4")
	if err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	if _, err := af.Start(context.Background(), "st-4"); err == nil {
		t.Fatal("second concurrent start must be rejected")
	}
	current, ok := af.Session("st-4")
	if !ok || current.ID != first.ID {
		t.Fatal("original session must remain the active one")
	}
	if runner.Running("st-4") == false {
		t.Fatal("first worker should still be running")
	}
	if _, err := af.Stop("st-4"); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestLateStartSignalAfterTimeoutCannotKillNewSession(t *testing.T) {
	runner := &fakeRunner{}
	af := newTestAntifreeze(runner)

	// First attempt ignores cancellation and signals success well after the
	// startup timeout has elapsed.
	runner.enqueue(func(ctx context.Context, started chan<- error) {
		select {
		case <-time.After(60 * time.Millisecond):
			close(started)
			<-ctx.Done()
		case <-ctx.Done():
		}
	})
	runner.enqueue(succeedThenRun)

	if _, err := af.Start(context.Background(), "st-5"); err == nil {
		t.Fatal("expected timeout error for first attempt")
	}

	second, err := af.Start(context.Background(), "st-5")
	if err != nil {
		t.Fatalf("retry after timeout failed: %v", err)
	}

	// Wait long enough for the first worker's late startup signal to land.
	time.Sleep(100 * time.Millisecond)
	current, ok := af.Session("st-5")
	if !ok {
		t.Fatal("new session must remain registered despite the stale worker")
	}
	if current.ID != second.ID {
		t.Fatal("stale worker must not have replaced the active session")
	}
	if _, err := af.Stop("st-5"); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestCanceledRequestRejectedBeforeStart(t *testing.T) {
	runner := &fakeRunner{}
	af := newTestAntifreeze(runner)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := af.Start(ctx, "st-6"); err == nil {
		t.Fatal("start with already-canceled request context must fail")
	}
	if _, ok := af.Session("st-6"); ok {
		t.Fatal("nothing should be registered on rejected start")
	}
	waitFor(t, func() bool { return !runner.Running("st-6") }, "no worker should be left running")
}
