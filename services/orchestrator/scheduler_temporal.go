package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"opensoftwarefactory/orchestrator/pipeline"
)

// scheduleHashMemo is the memo key of the action that holds the hash of the config entry.
// It is on the action, not the schedule, because Temporal cannot update a schedule's memo.
const scheduleHashMemo = "factory_schedule_hash"

// minCatchupWindow is the smallest catch-up window Temporal accepts ("can't be less than 10
// seconds" in the SchedulePolicies proto); catchup_window: 0 maps to it, because the SDK
// leaves a zero window unset and Temporal then uses its default of one year.
const minCatchupWindow = 10 * time.Second

// scheduleOptions maps a config entry to a Temporal schedule. The action ID is the
// schedule ID; the SDK has no way to set keep_original_workflow_id, so Temporal always
// appends "-<nominal time in RFC 3339>" to it, which scheduledTime reads back.
func scheduleOptions(d desiredSchedule) (client.ScheduleOptions, error) {
	loc, err := d.Schedule.location()
	if err != nil {
		return client.ScheduleOptions{}, err
	}
	window, err := d.Schedule.catchup()
	if err != nil {
		return client.ScheduleOptions{}, err
	}
	return client.ScheduleOptions{
		ID:            d.ID,
		Spec:          client.ScheduleSpec{CronExpressions: []string{d.Schedule.Cron}, TimeZoneName: loc.String()},
		Overlap:       enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		CatchupWindow: max(window, minCatchupWindow),
		Paused:        d.Schedule.Paused,
		Action: &client.ScheduleWorkflowAction{
			ID:        d.ID,
			Workflow:  scheduleWorkflowName,
			Args:      []any{ScheduledRunInput{Schedule: d.Schedule.Name}},
			TaskQueue: pipeline.TaskQueue,
			Memo:      map[string]any{scheduleHashMemo: d.Hash},
		},
	}, nil
}

// temporalSchedules is scheduleAPI over the Temporal schedule client.
type temporalSchedules struct{ c client.Client }

// List lists the schedules with the prefix and describes each one, because a list entry
// carries neither the action memo nor a consistent paused flag.
func (t temporalSchedules) List(ctx context.Context, prefix string) ([]scheduleState, error) {
	it, err := t.c.ScheduleClient().List(ctx, client.ScheduleListOptions{})
	if err != nil {
		return nil, err
	}
	var out []scheduleState
	for it.HasNext() {
		entry, err := it.Next()
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(entry.ID, prefix) {
			continue
		}
		desc, err := t.c.ScheduleClient().GetHandle(ctx, entry.ID).Describe(ctx)
		if isNotFound(err) {
			continue // deleted since the list, which is eventually consistent
		} else if err != nil {
			return nil, err
		}
		st := scheduleState{ID: entry.ID, NextFires: desc.Info.NextActionTimes}
		if s := desc.Schedule.State; s != nil {
			st.Paused = s.Paused
		}
		if a, ok := desc.Schedule.Action.(*client.ScheduleWorkflowAction); ok {
			// A hash that cannot be read stays empty, so the schedule is updated.
			if p, ok := a.Memo[scheduleHashMemo].(*commonpb.Payload); ok {
				_ = converter.GetDefaultDataConverter().FromPayload(p, &st.Hash)
			}
		}
		if n := len(desc.Info.RecentActions); n > 0 {
			last := desc.Info.RecentActions[n-1].ScheduleTime
			st.LastFire = &last
		}
		out = append(out, st)
	}
	return out, nil
}

// Create creates the schedule; one that exists already, but was not yet listed, is updated.
func (t temporalSchedules) Create(ctx context.Context, d desiredSchedule) error {
	o, err := scheduleOptions(d)
	if err != nil {
		return err
	}
	_, err = t.c.ScheduleClient().Create(ctx, o)
	if errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return t.Update(ctx, d)
	}
	return err
}

// Update replaces the spec, action and policies, and then pauses or unpauses the schedule
// when its state differs from the config.
func (t temporalSchedules) Update(ctx context.Context, d desiredSchedule) error {
	o, err := scheduleOptions(d)
	if err != nil {
		return err
	}
	h := t.c.ScheduleClient().GetHandle(ctx, d.ID)
	var paused bool
	err = h.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
		s := in.Description.Schedule
		if s.State == nil {
			s.State = &client.ScheduleState{}
		}
		paused = s.State.Paused
		s.Spec, s.Action = &o.Spec, o.Action
		s.Policy = &client.SchedulePolicies{Overlap: o.Overlap, CatchupWindow: o.CatchupWindow}
		return &client.ScheduleUpdate{Schedule: &s}, nil
	}})
	if err != nil || paused == d.Schedule.Paused {
		return err
	}
	if d.Schedule.Paused {
		return h.Pause(ctx, client.SchedulePauseOptions{Note: "paused in the factory config"})
	}
	return h.Unpause(ctx, client.ScheduleUnpauseOptions{Note: "unpaused in the factory config"})
}

func (t temporalSchedules) Delete(ctx context.Context, id string) error {
	h, err := t.factoryHandle(ctx, id)
	if err != nil {
		return err
	}
	return notFound(h.Delete(ctx))
}

func (t temporalSchedules) Trigger(ctx context.Context, id string) error {
	h, err := t.factoryHandle(ctx, id)
	if err != nil {
		return err
	}
	return notFound(h.Trigger(ctx, client.ScheduleTriggerOptions{}))
}

// factoryHandle refuses a schedule the factory does not own.
func (t temporalSchedules) factoryHandle(ctx context.Context, id string) (client.ScheduleHandle, error) {
	if !strings.HasPrefix(id, schedulePrefix) {
		return nil, fmt.Errorf("schedule %q is not a factory schedule", id)
	}
	return t.c.ScheduleClient().GetHandle(ctx, id), nil
}

func isNotFound(err error) bool {
	var nf *serviceerror.NotFound
	return errors.As(err, &nf)
}

func notFound(err error) error {
	if isNotFound(err) {
		return errScheduleNotFound
	}
	return err
}
