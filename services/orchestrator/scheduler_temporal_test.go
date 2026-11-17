package main

import (
	"context"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

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
