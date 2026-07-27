package diagnostics

import "time"

type StepStatus string

const (
	StatusSuccess StepStatus = "success"
	StatusWarning StepStatus = "warning"
	StatusFailed  StepStatus = "failed"
	StatusSkipped StepStatus = "skipped"
)

type ResultStatus string

const (
	ResultConnected ResultStatus = "connected"
	ResultWarning   ResultStatus = "warning"
	ResultFailed    ResultStatus = "failed"
)

type Step struct {
	Name       string     `json:"name"`
	Status     StepStatus `json:"status"`
	DurationMS int64      `json:"duration_ms,omitempty"`
	Message    string     `json:"message,omitempty"`
	Suggestion string     `json:"suggestion,omitempty"`
}

type Result struct {
	Service    string       `json:"service"`
	Status     ResultStatus `json:"status"`
	Success    bool         `json:"success"`
	DurationMS int64        `json:"duration_ms"`
	Summary    string       `json:"summary"`
	Steps      []Step       `json:"steps"`
}

type runner struct {
	result Result
	start  time.Time
	failed bool
}

func newRunner(service string) *runner {
	return &runner{
		result: Result{
			Service: service,
			Status:  ResultConnected,
			Success: true,
			Summary: "Connected",
			Steps:   []Step{},
		},
		start: time.Now(),
	}
}

func (r *runner) finish() Result {
	r.result.DurationMS = time.Since(r.start).Milliseconds()
	if r.failed {
		r.result.Status = ResultFailed
		r.result.Success = false
		if r.result.Summary == "" || r.result.Summary == "Connected" {
			r.result.Summary = "Diagnostics failed"
		}
		return r.result
	}
	for _, step := range r.result.Steps {
		if step.Status == StatusWarning {
			r.result.Status = ResultWarning
			r.result.Summary = "Connected with warnings"
			return r.result
		}
	}
	return r.result
}

func (r *runner) add(step Step) {
	r.result.Steps = append(r.result.Steps, step)
	if step.Status == StatusFailed {
		r.failed = true
		r.result.Summary = step.Message
	}
}

func timedStep(name string, fn func() (string, string, error)) Step {
	start := time.Now()
	message, suggestion, err := fn()
	step := Step{
		Name:       name,
		Status:     StatusSuccess,
		DurationMS: time.Since(start).Milliseconds(),
		Message:    message,
		Suggestion: suggestion,
	}
	if err != nil {
		step.Status = StatusFailed
		if step.Message == "" {
			step.Message = err.Error()
		}
	}
	return step
}
