package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const scheduleConfig = sampleConfig + `schedules:
  - name: weekly
    workflow: sample
    input: tasks/weekly.md
    cron: "0 9 * * 1"
`

const newSchedule = `{"name":"daily","workflow":"sample","input":"tasks/daily.md","cron":"0 8 * * *"}`

// newScheduleServer serves the API over a fake Temporal client; a nil api means no scheduler.
func newScheduleServer(t *testing.T, api *fakeSchedules) (string, *memStore, string, *scheduler) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(scheduleConfig), 0o644); err != nil {
		t.Fatal(err)
	}
	store := newMemStore()
	s := &server{configPath: path, blobs: store, runner: &fakeRunner{}, logger: slog.New(slog.DiscardHandler)}
	if api != nil {
		s.scheduler = &scheduler{api: api, logger: s.logger}
	}
	srv := httptest.NewServer(s.routes())
	t.Cleanup(srv.Close)
	return srv.URL, store, path, s.scheduler
}

func listSchedules(t *testing.T, url string) []map[string]any {
	t.Helper()
	code, body := call(t, http.MethodGet, url+"/api/schedules", "")
	if code != http.StatusOK {
		t.Fatalf("GET = %d %s", code, body)
	}
	var got []map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestListSchedulesShape(t *testing.T) {
	fire := time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)
	next := fire.Add(7 * 24 * time.Hour)
	api := &fakeSchedules{have: []scheduleState{{ID: "factory/weekly", Paused: true, NextFires: []time.Time{next}, LastFire: &fire}}}
	url, store, _, _ := newScheduleServer(t, api)
	store.data["schedules/weekly/20261116T090000Z.json"] = []byte(`{"outcome":"failed","trace_id":"t1","error":"input missing"}`)
	got := listSchedules(t, url)
	if len(got) != 1 {
		t.Fatalf("got %v", got)
	}
	s := got[0]
	last, _ := s["last_fire"].(map[string]any)
	next0, _ := s["next_fires"].([]any)
	if s["name"] != "weekly" || s["workflow"] != "sample" || s["input"] != "tasks/weekly.md" || s["cron"] != "0 9 * * 1" ||
		s["timezone"] != "" || s["catchup_window"] != "" || s["paused"] != false || s["state"] != "paused" ||
		len(next0) != 1 || next0[0] != "2026-11-23T09:00:00Z" ||
		last["time"] != "2026-11-16T09:00:00Z" || last["outcome"] != "failed" || last["trace_id"] != "t1" || last["error"] != "input missing" {
		t.Fatalf("schedule = %v", s)
	}
}

func TestListSchedulesWithoutRecordOrState(t *testing.T) {
	fire := time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)
	api := &fakeSchedules{have: []scheduleState{{ID: "factory/weekly", LastFire: &fire}}}
	url, _, _, _ := newScheduleServer(t, api)
	s := listSchedules(t, url)[0]
	last, _ := s["last_fire"].(map[string]any)
	if s["state"] != "active" || last["time"] != "2026-11-16T09:00:00Z" || last["outcome"] != "" || last["trace_id"] != "" || last["error"] != "" {
		t.Fatalf("schedule = %v", s)
	}
	api.have = nil
	s = listSchedules(t, url)[0]
	if s["state"] != "pending" || s["last_fire"] != nil || len(s["next_fires"].([]any)) != 0 {
		t.Fatalf("pending schedule = %v", s)
	}
}

func TestListSchedulesSurvivesFailingList(t *testing.T) {
	url, _, _, _ := newScheduleServer(t, &fakeSchedules{listErr: errors.New("temporal slow")})
	if s := listSchedules(t, url)[0]; s["state"] != "pending" {
		t.Fatalf("schedule = %v", s)
	}
}

func TestSchedulesWithoutSchedulerAre503(t *testing.T) {
	url, _, _, _ := newScheduleServer(t, nil)
	for _, r := range [][2]string{
		{"GET", "/api/schedules"}, {"POST", "/api/schedules"}, {"DELETE", "/api/schedules/weekly"},
		{"POST", "/api/schedules/weekly/pause"}, {"POST", "/api/schedules/weekly/unpause"}, {"POST", "/api/schedules/weekly/trigger"},
	} {
		code, body := call(t, r[0], url+r[1], newSchedule)
		if code != http.StatusServiceUnavailable || !strings.Contains(body, "scheduling is not configured") {
			t.Errorf("%v = %d %s", r, code, body)
		}
	}
}

func TestCreateSchedule(t *testing.T) {
	url, _, path, sc := newScheduleServer(t, &fakeSchedules{})
	code, body := call(t, http.MethodPost, url+"/api/schedules", newSchedule)
	if code != http.StatusCreated || !strings.Contains(body, `"name":"daily"`) {
		t.Fatalf("create = %d %s", code, body)
	}
	if c, _ := loadConfig(path); len(c.Schedules) != 2 || c.Schedules[1].Name != "daily" {
		t.Fatalf("schedules = %v", c.Schedules)
	}
	select {
	case <-sc.channel():
	default:
		t.Fatal("create did not kick the scheduler")
	}
}

func TestCreateScheduleRefusals(t *testing.T) {
	url, _, path, sc := newScheduleServer(t, &fakeSchedules{})
	for _, tc := range []struct {
		body, want string
		code       int
	}{
		{`{"name":"bad","workflow":"sample","input":"x","cron":"* * * *"}`, "cron", 422},
		{`{"name":"bad","workflow":"sample","input":"x","cron":"* * * * *","timezone":"Mars/Base"}`, "timezone", 422},
		{`{"name":"weekly","workflow":"sample","input":"x","cron":"* * * * *"}`, "already exists", 409},
		{`{"name":"x","bogus":1}`, "bogus", 400},
	} {
		code, body := call(t, http.MethodPost, url+"/api/schedules", tc.body)
		if code != tc.code || !strings.Contains(body, tc.want) {
			t.Errorf("%s = %d %s", tc.body, code, body)
		}
	}
	if c, _ := loadConfig(path); len(c.Schedules) != 1 {
		t.Fatalf("schedules = %v", c.Schedules)
	}
	select {
	case <-sc.channel():
		t.Fatal("a refused create kicked the scheduler")
	default:
	}
}

func TestDeletePauseUnpauseSchedule(t *testing.T) {
	url, _, path, sc := newScheduleServer(t, &fakeSchedules{})
	kicked := func() bool {
		select {
		case <-sc.channel():
			return true
		default:
			return false
		}
	}
	code, body := call(t, http.MethodPost, url+"/api/schedules/weekly/pause", "")
	if c, _ := loadConfig(path); code != http.StatusOK || !strings.Contains(body, `"paused":true`) || !c.Schedules[0].Paused || !kicked() {
		t.Fatalf("pause = %d %s", code, body)
	}
	code, body = call(t, http.MethodPost, url+"/api/schedules/weekly/unpause", "")
	if c, _ := loadConfig(path); code != http.StatusOK || !strings.Contains(body, `"paused":false`) || c.Schedules[0].Paused || !kicked() {
		t.Fatalf("unpause = %d %s", code, body)
	}
	code, _ = call(t, http.MethodDelete, url+"/api/schedules/weekly", "")
	if c, _ := loadConfig(path); code != http.StatusNoContent || len(c.Schedules) != 0 || !kicked() {
		t.Fatalf("delete = %d", code)
	}
}

func TestUnknownScheduleIs404(t *testing.T) {
	url, _, _, sc := newScheduleServer(t, &fakeSchedules{})
	for _, r := range [][2]string{
		{"DELETE", "/api/schedules/nope"}, {"POST", "/api/schedules/nope/pause"},
		{"POST", "/api/schedules/nope/unpause"}, {"POST", "/api/schedules/nope/trigger"},
	} {
		if code, body := call(t, r[0], url+r[1], ""); code != http.StatusNotFound {
			t.Errorf("%v = %d %s", r, code, body)
		}
	}
	select {
	case <-sc.channel():
		t.Fatal("a refused change kicked the scheduler")
	default:
	}
}

func TestTriggerUpstreamFailureIs502(t *testing.T) {
	api := &fakeSchedules{listErr: errors.New("temporal down")}
	url, _, _, _ := newScheduleServer(t, api)
	if code, body := call(t, http.MethodPost, url+"/api/schedules/weekly/trigger", ""); code != http.StatusBadGateway {
		t.Fatalf("trigger = %d %s", code, body)
	}
}

func TestTriggerSchedule(t *testing.T) {
	api := &fakeSchedules{}
	url, _, path, _ := newScheduleServer(t, api)
	code, body := call(t, http.MethodPost, url+"/api/schedules/weekly/trigger", "")
	if code != http.StatusConflict || !strings.Contains(body, "schedule is not active yet") || len(api.calls) != 0 {
		t.Fatalf("pending trigger = %d %s %v", code, body, api.calls)
	}
	api.have = []scheduleState{{ID: "factory/weekly", Paused: true}}
	code, body = call(t, http.MethodPost, url+"/api/schedules/weekly/trigger", "")
	if code != http.StatusAccepted || !strings.Contains(body, `"name":"weekly"`) || len(api.calls) != 1 || api.calls[0] != "trigger factory/weekly" {
		t.Fatalf("trigger = %d %s %v", code, body, api.calls)
	}
	if c, _ := loadConfig(path); c.Schedules[0].Paused {
		t.Fatal("trigger changed the config")
	}
}
