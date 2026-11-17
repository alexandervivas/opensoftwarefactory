package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"opensoftwarefactory/orchestrator/pipeline"
)

const fireConfig = sampleConfig + `schedules:
  - name: weekly
    workflow: sample
    input: tasks/weekly.md
    cron: "0 9 * * 1"
`

// dedupRunner answers a second start of the same trace ID the way Temporal does.
type dedupRunner struct{ *fakeRunner }

func (d dedupRunner) Start(ctx context.Context, in pipeline.Input) error {
	for _, s := range d.started {
		if s.TraceID == in.TraceID {
			return errAlreadyStarted
		}
	}
	return d.fakeRunner.Start(ctx, in)
}

func newFireServer(t *testing.T, config string) (*server, *fakeRunner, *memStore) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}
	runner, store := &fakeRunner{}, newMemStore()
	return &server{configPath: path, blobs: store, runner: dedupRunner{runner}, logger: slog.New(slog.DiscardHandler)}, runner, store
}

func fire(t *testing.T, s *server) (FireRecord, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	a := &scheduleActivities{s: s}
	env.RegisterActivity(a)
	val, err := env.ExecuteActivity(a.StartScheduledRun, ScheduledRunInput{Schedule: "weekly"})
	var rec FireRecord
	if err == nil {
		err = val.Get(&rec)
	}
	return rec, err
}

// fireRecords returns the fire records stored for the weekly schedule.
func fireRecords(t *testing.T, store *memStore) []FireRecord {
	t.Helper()
	var out []FireRecord
	for id, data := range store.data {
		if strings.HasPrefix(id, "schedules/weekly/") && strings.HasSuffix(id, ".json") {
			var rec FireRecord
			if err := json.Unmarshal(data, &rec); err != nil {
				t.Fatal(err)
			}
			out = append(out, rec)
		}
	}
	return out
}

func TestFireStartsRun(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans)))
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	s, runner, store := newFireServer(t, fireConfig)
	store.data["tasks/weekly.md"] = []byte("weekly task")
	rec, err := fire(t, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(runner.started) != 1 {
		t.Fatalf("started %d runs", len(runner.started))
	}
	trace := runner.started[0].TraceID
	var run RunRecord
	if err := json.Unmarshal(store.data["runs/"+trace+"/run.json"], &run); err != nil {
		t.Fatal(err)
	}
	if run.StartedBy != "schedule/weekly" || run.Schedule != "weekly" || run.Source != "tasks/weekly.md" || run.DistributedTraceID == "" {
		t.Fatalf("run record %+v", run)
	}
	if got := string(store.data[runner.started[0].Inputs[0].Artifact]); got != "weekly task" {
		t.Fatalf("input %q", got)
	}
	records := fireRecords(t, store)
	if len(records) != 1 || records[0].Outcome != "started" || records[0].TraceID != trace || records[0].Workflow != "sample@v1" || rec != records[0] {
		t.Fatalf("fire records %+v, returned %+v", records, rec)
	}
	ended := spans.GetSpans()
	if len(ended) != 1 || ended[0].Name != "orchestrator.scheduled_run" || ended[0].SpanContext.TraceID().String() != run.DistributedTraceID {
		t.Fatalf("spans %+v", ended)
	}
	attrs := map[string]string{}
	for _, kv := range ended[0].Attributes {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["factory.run_id"] != trace || attrs["factory.workflow"] != "sample" || attrs["factory.schedule"] != "weekly" {
		t.Fatalf("span attributes %v", attrs)
	}
}

func TestFireMissingInput(t *testing.T) {
	s, runner, store := newFireServer(t, fireConfig)
	_, err := fire(t, s)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() || !strings.Contains(err.Error(), "tasks/weekly.md") {
		t.Fatalf("want a non-retryable error naming the input, got %v", err)
	}
	records := fireRecords(t, store)
	if len(runner.started) != 0 || len(records) != 1 || records[0].Outcome != "failed" || !strings.Contains(records[0].Error, "tasks/weekly.md") {
		t.Fatalf("started %d, records %+v", len(runner.started), records)
	}
}

func TestFireUnknownWorkflow(t *testing.T) {
	for name, config := range map[string]string{
		// The schedule left the configuration before the reconciler removed it from Temporal.
		"schedule removed": sampleConfig,
		// A hand edit renamed the workflow, so the configuration no longer loads.
		"workflow gone": strings.Replace(fireConfig, "workflow: sample", "workflow: gone", 1),
	} {
		t.Run(name, func(t *testing.T) {
			s, runner, store := newFireServer(t, config)
			store.data["tasks/weekly.md"] = []byte("weekly task")
			if _, err := fire(t, s); err == nil {
				t.Fatal("fire succeeded")
			}
			records := fireRecords(t, store)
			if len(runner.started) != 0 || len(records) != 1 || records[0].Outcome != "failed" || records[0].Error == "" {
				t.Fatalf("started %d, records %+v", len(runner.started), records)
			}
		})
	}
}

func TestFireRetryIsIdempotent(t *testing.T) {
	s, runner, store := newFireServer(t, fireConfig)
	store.data["tasks/weekly.md"] = []byte("weekly task")
	first, err := fire(t, s)
	if err != nil {
		t.Fatal(err)
	}
	second, err := fire(t, s)
	if err != nil {
		t.Fatal(err)
	}
	if first.TraceID != second.TraceID || len(runner.started) != 1 || second.Outcome != "started" {
		t.Fatalf("first %+v, second %+v, %d runs", first, second, len(runner.started))
	}
}

func TestScheduledRunWorkflow(t *testing.T) {
	run := func(t *testing.T, input bool) (*testsuite.TestWorkflowEnvironment, *memStore) {
		s, _, store := newFireServer(t, fireConfig)
		if input {
			store.data["tasks/weekly.md"] = []byte("weekly task")
		}
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflowWithOptions(ScheduledRun, workflow.RegisterOptions{Name: scheduleWorkflowName})
		env.RegisterActivity(&scheduleActivities{s: s})
		env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "factory/weekly-2026-11-23T09:00:00Z"})
		env.ExecuteWorkflow(scheduleWorkflowName, ScheduledRunInput{Schedule: "weekly"})
		return env, store
	}
	t.Run("missing input fails the workflow", func(t *testing.T) {
		env, store := run(t, false)
		if !env.IsWorkflowCompleted() || env.GetWorkflowError() == nil {
			t.Fatal("workflow did not fail")
		}
		if _, ok := store.data["schedules/weekly/20261123T090000Z.json"]; !ok {
			t.Fatal("no fire record at the scheduled time")
		}
	})
	t.Run("success returns the record", func(t *testing.T) {
		env, _ := run(t, true)
		var rec FireRecord
		if err := env.GetWorkflowResult(&rec); err != nil {
			t.Fatal(err)
		}
		if rec.Outcome != "started" || rec.ScheduledTime != "2026-11-23T09:00:00Z" || rec.TraceID != fireTraceID("factory/weekly-2026-11-23T09:00:00Z") {
			t.Fatalf("record %+v", rec)
		}
	})
}

func TestFireTraceIDStable(t *testing.T) {
	a, b := fireTraceID("factory/weekly-2026-11-23T09:00:00Z"), fireTraceID("factory/weekly-2026-11-23T09:00:00Z")
	if a != b || a == fireTraceID("factory/weekly-2026-11-30T09:00:00Z") {
		t.Fatalf("trace IDs %s %s", a, b)
	}
}

// Both kinds of run record who started them and where the input came from.
func TestRunRecordSourceAndStartedBy(t *testing.T) {
	srv, runner, store, _ := newTestServer(t)
	store.data["tasks/weekly.md"] = []byte("weekly task")
	if code, body := call(t, http.MethodPost, srv.URL+"/api/workflows/sample/runs?input=tasks/weekly.md", ""); code != http.StatusAccepted {
		t.Fatalf("%d %s", code, body)
	}
	var manual RunRecord
	if err := json.Unmarshal(store.data["runs/"+runner.started[0].TraceID+"/run.json"], &manual); err != nil {
		t.Fatal(err)
	}
	if manual.StartedBy != "orchestrator" || manual.Source != "tasks/weekly.md" || manual.Schedule != "" {
		t.Fatalf("manual record %+v", manual)
	}
}
