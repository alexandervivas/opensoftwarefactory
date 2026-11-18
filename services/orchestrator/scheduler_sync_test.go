package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeSchedules records the calls the reconciler makes, in order.
type fakeSchedules struct {
	mu      sync.Mutex
	have    []scheduleState
	listErr error
	lists   int
	calls   []string
	updated []desiredSchedule
}

func (f *fakeSchedules) List(_ context.Context, _ string) ([]scheduleState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	return f.have, f.listErr
}

func (f *fakeSchedules) record(call string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
	return nil
}

func (f *fakeSchedules) Create(_ context.Context, d desiredSchedule) error {
	return f.record("create " + d.ID)
}
func (f *fakeSchedules) Update(_ context.Context, d desiredSchedule) error {
	f.mu.Lock()
	f.updated = append(f.updated, d)
	f.mu.Unlock()
	return f.record("update " + d.ID)
}
func (f *fakeSchedules) Delete(_ context.Context, id string) error  { return f.record("delete " + id) }
func (f *fakeSchedules) Trigger(_ context.Context, id string) error { return f.record("trigger " + id) }

var weekly = Schedule{Name: "weekly", Workflow: "sample", Input: "tasks/weekly.md", Cron: "0 9 * * 1"}

func reconcileWith(t *testing.T, have []scheduleState, schedules ...Schedule) *fakeSchedules {
	t.Helper()
	f := &fakeSchedules{have: have}
	sc := &scheduler{api: f, logger: slog.New(slog.DiscardHandler)}
	if err := sc.reconcile(context.Background(), Config{Schedules: schedules}); err != nil {
		t.Fatal(err)
	}
	return f
}

func hashOf(t *testing.T, s Schedule) string {
	t.Helper()
	d, err := desired(s)
	if err != nil {
		t.Fatal(err)
	}
	return d.Hash
}

func TestReconcileCreatesMissing(t *testing.T) {
	f := reconcileWith(t, nil, weekly)
	if len(f.calls) != 1 || f.calls[0] != "create factory/weekly" {
		t.Fatalf("calls = %v", f.calls)
	}
}

func TestReconcileUpdatesOnlyChanged(t *testing.T) {
	same := reconcileWith(t, []scheduleState{{ID: "factory/weekly", Hash: hashOf(t, weekly)}}, weekly)
	if len(same.calls) != 0 {
		t.Fatalf("unchanged schedule: calls = %v", same.calls)
	}
	changed := weekly
	changed.Cron = "0 10 * * 1"
	f := reconcileWith(t, []scheduleState{{ID: "factory/weekly", Hash: hashOf(t, weekly)}}, changed)
	if len(f.calls) != 1 || f.calls[0] != "update factory/weekly" || f.updated[0].Schedule.Cron != "0 10 * * 1" {
		t.Fatalf("changed schedule: calls = %v", f.calls)
	}
}

func TestReconcileDeletesOnlyFactorySchedules(t *testing.T) {
	f := reconcileWith(t, []scheduleState{{ID: "factory/gone"}, {ID: "nightly-backup"}, {ID: "factoryish"}})
	if len(f.calls) != 1 || f.calls[0] != "delete factory/gone" {
		t.Fatalf("calls = %v", f.calls)
	}
	// A schedule of someone else with a configured name is not ours either.
	f = reconcileWith(t, []scheduleState{{ID: "weekly", Hash: "x"}}, weekly)
	if len(f.calls) != 1 || f.calls[0] != "create factory/weekly" {
		t.Fatalf("calls = %v", f.calls)
	}
}

func TestReconcilePausedFlag(t *testing.T) {
	paused := weekly
	paused.Paused = true
	if hashOf(t, paused) != hashOf(t, weekly) {
		t.Fatal("the paused flag must not change the hash, so a pause is applied on its own")
	}
	for _, tc := range []struct {
		name   string
		config Schedule
		state  bool
	}{{"pause", paused, false}, {"unpause", weekly, true}} {
		f := reconcileWith(t, []scheduleState{{ID: "factory/weekly", Hash: hashOf(t, weekly), Paused: tc.state}}, tc.config)
		if len(f.updated) != 1 || f.updated[0].Schedule.Paused != tc.config.Paused {
			t.Fatalf("%s: calls = %v", tc.name, f.calls)
		}
	}
}

func TestReconcileListError(t *testing.T) {
	f := &fakeSchedules{listErr: errors.New("temporal unreachable")}
	sc := &scheduler{api: f, logger: slog.New(slog.DiscardHandler)}
	if err := sc.reconcile(context.Background(), Config{Schedules: []Schedule{weekly}}); err == nil {
		t.Fatal("want the List error")
	}
	if len(f.calls) != 0 {
		t.Fatalf("calls after a List error = %v", f.calls)
	}
}

func TestSchedulerRunKeepsTicking(t *testing.T) {
	f := &fakeSchedules{listErr: errors.New("temporal unreachable")}
	sc := &scheduler{api: f, logger: slog.New(slog.DiscardHandler)}
	loads := 0
	load := func() (Config, error) {
		loads++
		if loads == 1 {
			return Config{}, errors.New("config.yaml is invalid")
		}
		return Config{Schedules: []Schedule{weekly}}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sc.run(ctx, load, time.Millisecond); close(done) }()
	// Temporal is unreachable for two ticks and back on a later one.
	waitFor(t, func() bool {
		if f.lists >= 2 {
			f.listErr = nil
		}
		return len(f.calls) > 0
	}, f)
	cancel()
	<-done
	if f.calls[0] != "create factory/weekly" {
		t.Fatalf("calls = %v", f.calls)
	}
}

func waitFor(t *testing.T, cond func() bool, f *fakeSchedules) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		f.mu.Lock()
		ok := cond()
		f.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
	}
}

func TestKickReconcilesWithoutWaitingForTheTick(t *testing.T) {
	f := &fakeSchedules{}
	sc := &scheduler{api: f, logger: slog.New(slog.DiscardHandler)}
	var mu sync.Mutex
	cfg := Config{}
	load := func() (Config, error) { mu.Lock(); defer mu.Unlock(); return cfg, nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sc.run(ctx, load, time.Hour); close(done) }()
	waitFor(t, func() bool { return f.lists >= 1 }, f)
	mu.Lock()
	cfg = Config{Schedules: []Schedule{weekly}}
	mu.Unlock()
	finished := make(chan struct{})
	go func() { sc.kick(); sc.kick(); sc.kick(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("kick blocked")
	}
	waitFor(t, func() bool { return len(f.calls) > 0 }, f)
	cancel()
	<-done
	if f.calls[0] != "create factory/weekly" {
		t.Fatalf("calls = %v", f.calls)
	}
}
