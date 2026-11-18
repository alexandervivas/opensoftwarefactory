package main

import (
	"encoding/json"
	"net/http"
	"time"
)

// scheduleFields is a Schedule on the wire, with every key present even when empty.
type scheduleFields struct {
	Name          string `json:"name"`
	Workflow      string `json:"workflow"`
	Input         string `json:"input"`
	Cron          string `json:"cron"`
	Timezone      string `json:"timezone"`
	CatchupWindow string `json:"catchup_window"`
	Paused        bool   `json:"paused"`
}

// scheduleView is a configured schedule with what Temporal and the fire records know of it.
type scheduleView struct {
	scheduleFields
	State     string    `json:"state"`
	NextFires []string  `json:"next_fires"`
	LastFire  *fireView `json:"last_fire"`
}

type fireView struct {
	Time    string `json:"time"`
	Outcome string `json:"outcome"`
	TraceID string `json:"trace_id"`
	Error   string `json:"error"`
}

// withScheduler answers 503 when scheduling is not configured.
func (s *server) withScheduler(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.scheduler == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "scheduling is not configured"})
			return
		}
		h(w, r)
	}
}

// getSchedules lists the configured schedules. A Temporal that cannot be read leaves every
// schedule pending instead of failing the list.
func (s *server) getSchedules(w http.ResponseWriter, r *http.Request) {
	c, err := s.config()
	if err != nil {
		s.fail(w, http.StatusInternalServerError, err)
		return
	}
	states := map[string]scheduleState{}
	have, err := s.scheduler.api.List(r.Context(), schedulePrefix)
	if err != nil {
		s.logger.Error("schedules not read from Temporal", "error", err)
	}
	for _, st := range have {
		states[st.ID] = st
	}
	views := []scheduleView{}
	for _, sch := range c.Schedules {
		v := scheduleView{scheduleFields: scheduleFields(sch), State: "pending", NextFires: []string{}}
		if st, ok := states[schedulePrefix+sch.Name]; ok {
			v.State = "active"
			if st.Paused {
				v.State = "paused"
			}
			for _, t := range st.NextFires {
				v.NextFires = append(v.NextFires, t.UTC().Format(time.RFC3339))
			}
			if st.LastFire != nil {
				v.LastFire = s.fireView(r, sch.Name, st.LastFire.UTC())
			}
		}
		views = append(views, v)
	}
	writeJSON(w, http.StatusOK, views)
}

// fireView reads the record of a fire; a missing or unreadable record leaves only the time.
func (s *server) fireView(r *http.Request, name string, at time.Time) *fireView {
	v := &fireView{Time: at.Format(time.RFC3339)}
	data, err := s.blobs.Get(r.Context(), "schedules/"+name+"/"+at.Format("20060102T150405Z")+".json")
	if err != nil {
		return v
	}
	var rec FireRecord
	if json.Unmarshal(data, &rec) == nil {
		v.Outcome, v.TraceID, v.Error = rec.Outcome, rec.TraceID, rec.Error
	}
	return v
}
