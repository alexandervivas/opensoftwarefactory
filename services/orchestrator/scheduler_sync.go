package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// schedulePrefix marks the Temporal schedules the orchestrator owns; no other schedule is
// ever changed or deleted.
const schedulePrefix = "factory/"

var errScheduleNotFound = errors.New("schedule not found")

// scheduleAPI is the part of the Temporal schedule client the reconciler uses.
type scheduleAPI interface {
	List(ctx context.Context, prefix string) ([]scheduleState, error)
	Create(ctx context.Context, d desiredSchedule) error
	Update(ctx context.Context, d desiredSchedule) error
	Delete(ctx context.Context, id string) error
	Trigger(ctx context.Context, id string) error
}

// desiredSchedule is a config entry as a Temporal schedule.
type desiredSchedule struct {
	ID       string
	Schedule Schedule
	Hash     string
}

// scheduleState is a Temporal schedule as the reconciler and the API see it.
type scheduleState struct {
	ID, Hash  string
	Paused    bool
	NextFires []time.Time
	LastFire  *time.Time
}

type scheduler struct {
	api    scheduleAPI
	logger *slog.Logger
}

// desired hashes the config entry without its paused flag, so a pause or an unpause made
// outside the config is put back as well as an edit.
func desired(s Schedule) (desiredSchedule, error) {
	entry := s
	entry.Paused = false
	data, err := json.Marshal(entry)
	if err != nil {
		return desiredSchedule{}, err
	}
	sum := sha256.Sum256(data)
	return desiredSchedule{ID: schedulePrefix + s.Name, Schedule: s, Hash: hex.EncodeToString(sum[:])}, nil
}

// reconcile makes the factory schedules in Temporal match the config. A failure on one
// schedule does not stop the others; all failures are returned together.
func (sc *scheduler) reconcile(ctx context.Context, c Config) error {
	have, err := sc.api.List(ctx, schedulePrefix)
	if err != nil {
		return fmt.Errorf("listing schedules: %w", err)
	}
	current := map[string]scheduleState{}
	for _, st := range have {
		if strings.HasPrefix(st.ID, schedulePrefix) {
			current[st.ID] = st
		}
	}
	var errs []error
	wanted := map[string]bool{}
	for _, s := range c.Schedules {
		d, err := desired(s)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		wanted[d.ID] = true
		st, ok := current[d.ID]
		switch {
		case !ok:
			err = sc.api.Create(ctx, d)
		case st.Hash != d.Hash || st.Paused != s.Paused:
			err = sc.api.Update(ctx, d)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("schedule %q: %w", s.Name, err))
		}
	}
	for id := range current {
		if !wanted[id] {
			if err := sc.api.Delete(ctx, id); err != nil && !errors.Is(err, errScheduleNotFound) {
				errs = append(errs, fmt.Errorf("deleting %s: %w", id, err))
			}
		}
	}
	return errors.Join(errs...)
}

// run reconciles now and then on every tick until ctx ends. A config that does not load
// is skipped, so it never deletes schedules; every error is logged and retried next tick.
func (sc *scheduler) run(ctx context.Context, load func() (Config, error), every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		if c, err := load(); err != nil {
			sc.logger.Error("schedules not reconciled: config not loaded", "error", err)
		} else {
			rctx, cancel := context.WithTimeout(ctx, max(every, time.Second))
			if err := sc.reconcile(rctx, c); err != nil {
				sc.logger.Error("schedules not reconciled", "error", err)
			}
			cancel()
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
