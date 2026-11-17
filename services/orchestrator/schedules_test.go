package main

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const scheduleBase = `workflows:
  - name: ex05-architecture-review
    steps: [{id: a, service: agent, agent: demo/writer@v1}]
schedules:
`

func TestScheduleValidation(t *testing.T) {
	entry := func(over map[string]string) string {
		f := map[string]string{"name": "weekly", "workflow": "ex05-architecture-review", "input": "tasks/weekly.md", "cron": "0 9 * * 1"}
		for k, v := range over {
			f[k] = v
		}
		var b strings.Builder
		first := "  - "
		for _, k := range []string{"name", "workflow", "input", "cron", "timezone", "catchup_window"} {
			if v, ok := f[k]; ok {
				b.WriteString(first + k + ": " + `"` + v + `"` + "\n")
				first = "    "
			}
		}
		return b.String()
	}
	tests := []struct {
		name, yaml, wantErr string
	}{
		{"minimal", entry(nil), ""},
		{"pin", entry(map[string]string{"workflow": "ex05-architecture-review@v1"}), ""},
		{"zero catchup", entry(map[string]string{"catchup_window": "0"}), ""},
		{"timezone", entry(map[string]string{"timezone": "Europe/Amsterdam"}), ""},
		{"duplicate names differing in case", entry(nil) + entry(map[string]string{"name": "Weekly"}), "name"},
		{"upper name", entry(map[string]string{"name": "Weekly_X"}), "name"},
		{"long name", entry(map[string]string{"name": strings.Repeat("a", 64)}), "name"},
		{"unknown workflow", entry(map[string]string{"workflow": "nope"}), "workflow"},
		{"unknown version", entry(map[string]string{"workflow": "ex05-architecture-review@v9"}), "workflow"},
		{"bad input", entry(map[string]string{"input": "../x"}), "input"},
		{"empty input", entry(map[string]string{"input": ""}), "input"},
		{"four fields", entry(map[string]string{"cron": "* * * *"}), "cron"},
		{"cron range", entry(map[string]string{"cron": "61 * * * *"}), "cron"},
		{"bad timezone", entry(map[string]string{"timezone": "Mars/Base"}), "timezone"},
		{"negative catchup", entry(map[string]string{"catchup_window": "-1h"}), "catchup_window"},
		{"bad catchup", entry(map[string]string{"catchup_window": "soon"}), "catchup_window"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseConfig([]byte(scheduleBase + tt.yaml))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %v does not name %q", err, tt.wantErr)
			}
		})
	}
}

func TestScheduleDefaults(t *testing.T) {
	var s Schedule
	if d, err := s.catchup(); err != nil || d != 24*time.Hour {
		t.Errorf("catchup %v %v", d, err)
	}
	if l, err := s.location(); err != nil || l != time.UTC {
		t.Errorf("location %v %v", l, err)
	}
	s.CatchupWindow = "0"
	if d, err := s.catchup(); err != nil || d != 0 {
		t.Errorf("zero catchup %v %v", d, err)
	}
}

func TestEmptySchedulesKey(t *testing.T) {
	c, err := parseConfig([]byte("workflows:\n  - name: a\n    steps: [{id: a, service: svc}]\nschedules:\n"))
	if err != nil || len(c.Schedules) != 0 {
		t.Fatalf("%v %+v", err, c.Schedules)
	}
	if _, ok := c.findSchedule("x"); ok {
		t.Error("found a schedule in an empty list")
	}
}

const scheduledConfig = scheduleBase + `  - name: weekly
    workflow: ex05-architecture-review@v1
    input: tasks/weekly.md
    cron: "0 9 * * 1"
    paused: true
`

func TestSaveKeepsSchedules(t *testing.T) {
	srv, _, _, path := serverWith(t, scheduledConfig, nil)
	body := `{"steps":[{"id":"a","service":"svc"}]}`
	if code := send(t, http.MethodPost, srv.URL+"/api/workflows/other/versions", "", body, nil); code != http.StatusCreated {
		t.Fatalf("status %d", code)
	}
	c, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := c.findSchedule("weekly"); !ok || !s.Paused || s.Cron != "0 9 * * 1" {
		t.Fatalf("schedule lost: %+v", c.Schedules)
	}
}

func TestDeleteWorkflowUsedBySchedule(t *testing.T) {
	srv, _, _, path := serverWith(t, scheduledConfig, nil)
	var refused struct {
		Error string `json:"error"`
	}
	if code := send(t, http.MethodDelete, srv.URL+"/api/workflows/ex05-architecture-review", "", "", &refused); code != http.StatusConflict {
		t.Fatalf("status %d", code)
	}
	if refused.Error != `workflow "ex05-architecture-review" is used by schedule: weekly` {
		t.Errorf("error %q", refused.Error)
	}
	if c, err := loadConfig(path); err != nil || len(c.Workflows) != 1 {
		t.Errorf("workflow removed: %v", err)
	}
}

func TestRenameRewritesSchedule(t *testing.T) {
	srv, _, _, path := serverWith(t, scheduledConfig+`  - name: latest
    workflow: ex05-architecture-review
    input: tasks/weekly.md
    cron: "0 9 * * 1"
`, nil)
	if code := send(t, http.MethodPost, srv.URL+"/api/workflows/ex05-architecture-review/rename", "", `{"name":"review"}`, nil); code != http.StatusOK {
		data, _ := os.ReadFile(path)
		t.Fatalf("status %d\n%s", code, data)
	}
	c, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if s, _ := c.findSchedule("weekly"); s.Workflow != "review@v1" {
		t.Errorf("pinned: %q", s.Workflow)
	}
	if s, _ := c.findSchedule("latest"); s.Workflow != "review" {
		t.Errorf("bare: %q", s.Workflow)
	}
}
