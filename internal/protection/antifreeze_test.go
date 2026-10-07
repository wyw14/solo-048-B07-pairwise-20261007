package protection

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"heatpulse/internal/exchanger"
	"heatpulse/internal/pump"
)

func newTestAntifreeze() (*Antifreeze, *pump.Runner) {
	p := pump.NewRunner()
	return NewAntifreeze(exchanger.NewCirculationRunner(p)), p
}

func TestStartStopCycle(t *testing.T) {
	a, _ := newTestAntifreeze()
	defer a.Close()
	if _, err := a.Start(context.Background(), "s1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !a.Running("s1") {
		t.Fatal("worker not running after start")
	}
	if _, err := a.Start(context.Background(), "s1"); err == nil {
		t.Fatal("duplicate start should fail")
	}
	if _, err := a.Stop("s1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if a.Running("s1") {
		t.Fatal("worker still running after stop")
	}
	if _, err := a.Start(context.Background(), "s1"); err != nil {
		t.Fatalf("restart after clean stop: %v", err)
	}
}

func TestFailedStartReleasesStation(t *testing.T) {
	a, p := newTestAntifreeze()
	defer a.Close()

	// Occupy the pump directly so the antifreeze worker fails at startup.
	rawReady := make(chan error, 1)
	rawCtx, rawCancel := context.WithCancel(context.Background())
	defer rawCancel()
	go p.Circulate(rawCtx, "s2", time.Hour, rawReady)
	if err := <-rawReady; err != nil {
		t.Fatal(err)
	}

	if _, err := a.Start(context.Background(), "s2"); err == nil {
		t.Fatal("start against occupied pump should fail")
	}
	if _, ok := a.Session("s2"); ok {
		t.Fatal("failed start left an active reservation")
	}

	rawCancel()
	deadline := time.Now().Add(time.Second)
	for a.Running("s2") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := a.Start(context.Background(), "s2"); err != nil {
		t.Fatalf("station stayed occupied after failed start: %v", err)
	}
}

func TestConcurrentStartSingleWinner(t *testing.T) {
	a, _ := newTestAntifreeze()
	defer a.Close()
	var wg sync.WaitGroup
	var winners int32
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := a.Start(context.Background(), "s3"); err == nil {
				atomic.AddInt32(&winners, 1)
			}
		}()
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("got %d winning starts, want 1", winners)
	}
}
