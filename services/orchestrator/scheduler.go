package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"opensoftwarefactory/orchestrator/pipeline"
)

// scheduleWorkflowName is the workflow a Temporal schedule starts on each fire.
const scheduleWorkflowName = "ScheduledRun"

// ScheduledRunInput names the schedule that fired.
type ScheduledRunInput struct{ Schedule string }

// FireRecord is stored as schedules/<name>/<scheduled time>.json for every fire, so a
// failed fire leaves a record as well as a started one. Outcome is "started" or "failed".
type FireRecord struct {
	Schedule      string `json:"schedule"`
	ScheduledTime string `json:"scheduled_time"`
	Outcome       string `json:"outcome"`
	TraceID       string `json:"trace_id"`
	Workflow      string `json:"workflow"`
	Error         string `json:"error,omitempty"`
}

// errAlreadyStarted reports a start of a trace ID that already has a run.
var errAlreadyStarted = errors.New("run already started")

// ScheduledRun starts one pipeline run through a single activity. A missing input or an
// unknown schedule or workflow is not retried; other failures are retried twice.
func ScheduledRun(ctx workflow.Context, in ScheduledRunInput) (FireRecord, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var a *scheduleActivities
	var rec FireRecord
	err := workflow.ExecuteActivity(ctx, a.StartScheduledRun, in).Get(ctx, &rec)
	return rec, err
}

type scheduleActivities struct{ s *server }

// fireNamespace scopes the trace IDs derived from ScheduledRun workflow IDs.
var fireNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("opensoftwarefactory/scheduled-run"))

// fireTraceID derives the run's trace ID from the ScheduledRun workflow ID, so a retried
// activity starts the same run instead of a second one.
func fireTraceID(workflowID string) string {
	return uuid.NewSHA1(fireNamespace, []byte(workflowID)).String()
}

// scheduledTime reads the time Temporal appends to a scheduled workflow ID
// (<id>-2006-01-02T15:04:05Z); a workflow started otherwise uses the fallback.
func scheduledTime(workflowID string, fallback time.Time) time.Time {
	const suffix = len("2006-01-02T15:04:05Z")
	if len(workflowID) > suffix {
		if t, err := time.Parse(time.RFC3339, workflowID[len(workflowID)-suffix:]); err == nil {
			return t.UTC()
		}
	}
	return fallback.UTC()
}

// StartScheduledRun starts the schedule's workflow on its input artifact under a root span
// and writes the fire record on both outcomes. A failed fire returns its error after the
// record is written.
func (a *scheduleActivities) StartScheduledRun(ctx context.Context, in ScheduledRunInput) (FireRecord, error) {
	info := activity.GetInfo(ctx)
	at := scheduledTime(info.WorkflowExecution.ID, info.StartedTime)
	rec := FireRecord{Schedule: in.Schedule, ScheduledTime: at.Format(time.RFC3339), TraceID: fireTraceID(info.WorkflowExecution.ID)}
	// A new root, like the HTTP span of a manual run, linked to the Temporal span that fired it.
	ctx, span := otel.Tracer("orchestrator").Start(ctx, "orchestrator.scheduled_run", trace.WithNewRoot(),
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithAttributes(attribute.String("factory.run_id", rec.TraceID), attribute.String("factory.schedule", in.Schedule)))
	defer span.End()
	err := a.fire(ctx, &rec)
	rec.Outcome = "started"
	if err != nil {
		rec.Outcome, rec.Error = "failed", err.Error()
		span.SetStatus(codes.Error, err.Error())
	}
	data, _ := json.MarshalIndent(rec, "", "  ")
	id := fmt.Sprintf("schedules/%s/%s.json", in.Schedule, at.Format("20060102T150405Z"))
	if perr := a.s.blobs.Put(ctx, id, data, pipeline.Meta{Creator: "schedule/" + in.Schedule, TraceID: rec.TraceID}); perr != nil {
		// A retry writes the record again; a started run is found by its trace ID.
		a.s.logger.Error("fire record not written", "schedule", in.Schedule, "error", perr)
		if err == nil {
			err = perr
		}
	}
	return rec, err
}

// fire resolves the schedule's workflow and input at fire time and starts the run.
func (a *scheduleActivities) fire(ctx context.Context, rec *FireRecord) error {
	c, err := a.s.config()
	if err != nil {
		return err
	}
	sched, ok := c.findSchedule(rec.Schedule)
	if !ok {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("schedule %q is not configured", rec.Schedule), "UnknownSchedule", nil)
	}
	rec.Workflow = sched.Workflow
	def, ok := pipeline.FindDefinition(c.Workflows, sched.Workflow)
	if !ok {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("workflow %q does not exist", sched.Workflow), "UnknownWorkflow", nil)
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("factory.workflow", def.Name))
	body, err := a.s.blobs.Get(ctx, sched.Input)
	if errors.Is(err, pipeline.ErrNotFound) {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("input artifact %q not found", sched.Input), "MissingInput", err)
	} else if err != nil {
		return err
	}
	started, err := a.s.startRunFor(ctx, c, def, body, startOptions{
		StartedBy: "schedule/" + sched.Name, Source: sched.Input, Schedule: sched.Name, TraceID: rec.TraceID,
	})
	if err != nil {
		return err
	}
	rec.Workflow = pipeline.WorkflowRef(started.Definition.Name, started.Definition.Version)
	return nil
}
