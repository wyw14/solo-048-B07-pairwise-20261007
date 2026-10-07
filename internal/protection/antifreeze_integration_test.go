package protection

import (
	"context"
	"testing"

	"heatpulse/internal/exchanger"
	"heatpulse/internal/pump"
)

func newRealAntifreeze() *Antifreeze {
	return NewAntifreeze(exchanger.NewCirculationRunner(pump.NewRunner()))
}

func TestStartStopCycleWithRealRunner(t *testing.T) {
	af := newRealAntifreeze()

	session, err := af.Start(context.Background(), "real-1")
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if session.ID == "" || !session.Active {
		t.Fatalf("unexpected session: %+v", session)
	}
	if !af.Running("real-1") {
		t.Fatal("circulation should be running")
	}
	if _, err := af.Start(context.Background(), "real-1"); err == nil {
		t.Fatal("duplicate start must be rejected")
	}

	stopped, err := af.Stop("real-1")
	if err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	if stopped.Active {
		t.Fatal("session should be inactive after stop")
	}
	if af.Running("real-1") {
		t.Fatal("circulation should have stopped")
	}

	// The station can be started again after a clean stop.
	if _, err := af.Start(context.Background(), "real-1"); err != nil {
		t.Fatalf("restart failed: %v", err)
	}
	if _, err := af.Stop("real-1"); err != nil {
		t.Fatalf("second stop failed: %v", err)
	}
}

func TestCloseStopsAllSessions(t *testing.T) {
	af := newRealAntifreeze()
	for _, id := range []string{"c-1", "c-2"} {
		if _, err := af.Start(context.Background(), id); err != nil {
			t.Fatalf("start %s failed: %v", id, err)
		}
	}
	af.Close()
	waitFor(t, func() bool {
		return !af.Running("c-1") && !af.Running("c-2")
	}, "Close should stop every circulation")
	if _, ok := af.Session("c-1"); ok {
		t.Fatal("Close must unregister all sessions")
	}
}
