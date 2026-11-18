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

func (s *server) postSchedule(w http.ResponseWriter, r *http.Request) {
	var sch Schedule
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	d.DisallowUnknownFields()
	if err := d.Decode(&sch); err != nil {
		s.fail(w, http.StatusBadRequest, err)
		return
	}
	err := s.change(func(c *Config) error {
		if _, ok := c.findSchedule(sch.Name); ok {
			return refuse(http.StatusConflict, "schedule %q already exists", sch.Name)
		}
		c.Schedules = append(c.Schedules, sch)
		return nil
	})
	if err != nil {
		s.failChange(w, err)
		return
	}
	s.scheduler.kick()
	writeJSON(w, http.StatusCreated, scheduleFields(sch))
}

func (s *server) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	err := s.change(func(c *Config) error {
		for i, sch := range c.Schedules {
			if sch.Name == name {
				c.Schedules = append(c.Schedules[:i:i], c.Schedules[i+1:]...)
				return nil
			}
		}
		return refuse(http.StatusNotFound, "unknown schedule")
	})
	if err != nil {
		s.failChange(w, err)
		return
	}
	s.scheduler.kick()
	w.WriteHeader(http.StatusNoContent)
}

// pauseSchedule sets the paused flag in the config; the reconciler then pauses Temporal.
func (s *server) pauseSchedule(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var stored Schedule
		err := s.change(func(c *Config) error {
			for i := range c.Schedules {
				if c.Schedules[i].Name == name {
					c.Schedules[i].Paused = paused
					stored = c.Schedules[i]
					return nil
				}
			}
			return refuse(http.StatusNotFound, "unknown schedule")
		})
		if err != nil {
			s.failChange(w, err)
			return
		}
		s.scheduler.kick()
		writeJSON(w, http.StatusOK, scheduleFields(stored))
	}
}

// triggerSchedule fires a schedule now. A paused schedule may be triggered; one Temporal
// does not have yet may not. A failing Temporal List or Trigger is an upstream failure, so
// it answers 502, while an unknown name is 404 and a pending schedule 409.
func (s *server) triggerSchedule(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	c, err := s.config()
	if err != nil {
		s.fail(w, http.StatusInternalServerError, err)
		return
	}
	if _, ok := c.findSchedule(name); !ok {
		s.failChange(w, refuse(http.StatusNotFound, "unknown schedule"))
		return
	}
	have, err := s.scheduler.api.List(r.Context(), schedulePrefix)
	if err != nil {
		s.fail(w, http.StatusBadGateway, err)
		return
	}
	active := false
	for _, st := range have {
		active = active || st.ID == schedulePrefix+name
	}
	if !active {
		s.failChange(w, refuse(http.StatusConflict, "schedule is not active yet"))
		return
	}
	if err := s.scheduler.api.Trigger(r.Context(), schedulePrefix+name); err != nil {
		s.fail(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"name": name})
}
