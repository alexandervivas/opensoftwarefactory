package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/robfig/cron"

	"opensoftwarefactory/orchestrator/pipeline"
)

// Schedule starts a workflow on a cron expression, with the stored artifact Input as the
// run input. Workflow is a name or name@vN; a bare name runs the newest version at fire time.
type Schedule struct {
	Name          string `yaml:"name" json:"name"`
	Workflow      string `yaml:"workflow" json:"workflow"`
	Input         string `yaml:"input" json:"input"`
	Cron          string `yaml:"cron" json:"cron"`
	Timezone      string `yaml:"timezone,omitempty" json:"timezone,omitempty"`
	CatchupWindow string `yaml:"catchup_window,omitempty" json:"catchup_window,omitempty"`
	Paused        bool   `yaml:"paused,omitempty" json:"paused,omitempty"`
}

var scheduleNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// catchup is how far back a missed fire is still started: 24h by default, 0 for never.
func (s Schedule) catchup() (time.Duration, error) {
	if s.CatchupWindow == "" {
		return 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s.CatchupWindow)
	if err != nil {
		return 0, fmt.Errorf("catchup_window %q is not a duration such as 6h", s.CatchupWindow)
	}
	if d < 0 {
		return 0, fmt.Errorf("catchup_window %q must not be negative", s.CatchupWindow)
	}
	return d, nil
}

// location is the time zone the cron expression is read in, UTC by default.
func (s Schedule) location() (*time.Location, error) {
	if s.Timezone == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		return nil, fmt.Errorf("timezone %q is unknown", s.Timezone)
	}
	return loc, nil
}

func (c Config) findSchedule(name string) (Schedule, bool) {
	for _, s := range c.Schedules {
		if s.Name == name {
			return s, true
		}
	}
	return Schedule{}, false
}

// validateSchedules checks every schedule on its own and that no two share a name. Each
// error names the schedule and the field.
func (c Config) validateSchedules() error {
	seen := map[string]bool{}
	for _, s := range c.Schedules {
		fail := func(format string, args ...any) error {
			return fmt.Errorf("schedule %q: %s", s.Name, fmt.Sprintf(format, args...))
		}
		if !scheduleNamePattern.MatchString(s.Name) {
			return fail("name must match %s", scheduleNamePattern)
		}
		if seen[strings.ToLower(s.Name)] {
			return fail("name is used twice")
		}
		seen[strings.ToLower(s.Name)] = true
		name, version, ok := pipeline.ParseWorkflowRef(s.Workflow)
		if !ok {
			return fail("workflow %q is not a workflow name or name@vN", s.Workflow)
		}
		if _, found := c.find(name, version); !found {
			return fail("workflow %q does not exist", s.Workflow)
		}
		if !pipeline.ValidID(s.Input) {
			return fail("input %q is not a valid object id", s.Input)
		}
		// ParseStandard also accepts descriptors such as @daily; only 5 fields are allowed.
		if len(strings.Fields(s.Cron)) != 5 {
			return fail("cron %q must have 5 fields", s.Cron)
		}
		if _, err := cron.ParseStandard(s.Cron); err != nil {
			return fail("cron %q is invalid: %v", s.Cron, err)
		}
		if _, err := s.location(); err != nil {
			return fail("%v", err)
		}
		if _, err := s.catchup(); err != nil {
			return fail("%v", err)
		}
	}
	return nil
}
