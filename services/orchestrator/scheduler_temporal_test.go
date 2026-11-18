package main

import (
	"context"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"opensoftwarefactory/orchestrator/pipeline"
)

func TestSchedulerTemporalRefusesForeignIDs(t *testing.T) {
	// The Temporal client refuses a schedule without the prefix before it calls Temporal (its client is nil).
	if (temporalSchedules{}).Delete(context.Background(), "nightly-backup") == nil ||
		(temporalSchedules{}).Trigger(context.Background(), "nightly-backup") == nil {
		t.Fatal("a schedule without the factory/ prefix was not refused")
	}
}

func TestSchedulerSpecMapping(t *testing.T) {
	in := weekly
	in.Timezone = "Europe/Amsterdam"
	for _, tc := range []struct {
		window string
		want   time.Duration
	}{{"", 24 * time.Hour}, {"6h", 6 * time.Hour}, {"0", minCatchupWindow}} {
		in.CatchupWindow = tc.window
		d, err := desired(in)
		if err != nil {
			t.Fatal(err)
		}
		o, err := scheduleOptions(d)
		if err != nil {
			t.Fatal(err)
		}
		if o.ID != "factory/weekly" || o.CatchupWindow != tc.want || o.Overlap != enumspb.SCHEDULE_OVERLAP_POLICY_SKIP {
			t.Fatalf("window %q: options = %+v", tc.window, o)
		}
		if len(o.Spec.CronExpressions) != 1 || o.Spec.CronExpressions[0] != "0 9 * * 1" || o.Spec.TimeZoneName != "Europe/Amsterdam" {
			t.Fatalf("spec = %+v", o.Spec)
		}
		a := o.Action.(*client.ScheduleWorkflowAction)
		if a.Workflow != scheduleWorkflowName || a.TaskQueue != pipeline.TaskQueue || a.Memo[scheduleHashMemo] != d.Hash ||
			a.Args[0] != (ScheduledRunInput{Schedule: "weekly"}) {
			t.Fatalf("action = %+v", a)
		}
		// Temporal appends "-<nominal time>" to the action ID; the fire activity reads it back.
		at := time.Date(2026, 11, 23, 8, 0, 0, 0, time.UTC)
		if got := scheduledTime(a.ID+"-"+at.Format(time.RFC3339), time.Time{}); !got.Equal(at) {
			t.Fatalf("scheduled time from %q = %v", a.ID, got)
		}
	}
	d, _ := desired(weekly)
	if o, err := scheduleOptions(d); err != nil || o.Spec.TimeZoneName != "UTC" {
		t.Fatalf("default time zone = %q, %v", o.Spec.TimeZoneName, err)
	}
}

// fakeTemporal stands in for the parts of the Temporal client that List uses; any other
// method panics through the nil embedded interface.
type fakeTemporal struct {
	client.Client
	sc *fakeScheduleClient
}

func (f fakeTemporal) ScheduleClient() client.ScheduleClient { return f.sc }

type fakeScheduleClient struct {
	client.ScheduleClient
	ids   []string
	descs map[string]*client.ScheduleDescription
}

func (f *fakeScheduleClient) List(context.Context, client.ScheduleListOptions) (client.ScheduleListIterator, error) {
	return &fakeIterator{ids: f.ids}, nil
}
func (f *fakeScheduleClient) GetHandle(_ context.Context, id string) client.ScheduleHandle {
	return fakeHandle{desc: f.descs[id]}
}

type fakeIterator struct{ ids []string }

func (it *fakeIterator) HasNext() bool { return len(it.ids) > 0 }
func (it *fakeIterator) Next() (*client.ScheduleListEntry, error) {
	id := it.ids[0]
	it.ids = it.ids[1:]
	return &client.ScheduleListEntry{ID: id}, nil
}

type fakeHandle struct {
	client.ScheduleHandle
	desc *client.ScheduleDescription
}

func (h fakeHandle) Describe(context.Context) (*client.ScheduleDescription, error) {
	return h.desc, nil
}

func TestSchedulerTemporalListOnlyFactory(t *testing.T) {
	hash, err := converter.GetDefaultDataConverter().ToPayload("abc123")
	if err != nil {
		t.Fatal(err)
	}
	other := &client.ScheduleDescription{Schedule: client.Schedule{Action: &client.ScheduleWorkflowAction{}}}
	sc := &fakeScheduleClient{ids: []string{"nightly-backup", "factory/x"}, descs: map[string]*client.ScheduleDescription{
		"nightly-backup": other,
		"factory/x": {Schedule: client.Schedule{
			Action: &client.ScheduleWorkflowAction{Memo: map[string]any{scheduleHashMemo: hash}},
			State:  &client.ScheduleState{Paused: true},
		}},
	}}
	got, err := temporalSchedules{fakeTemporal{sc: sc}}.List(context.Background(), schedulePrefix)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "factory/x" || got[0].Hash != "abc123" || !got[0].Paused {
		t.Fatalf("List = %+v", got)
	}
}
