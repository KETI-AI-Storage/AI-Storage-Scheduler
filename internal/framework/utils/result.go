package utils

import (
	"errors"
	"math"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

type PluginResultMap map[string]PluginResult

type PluginResult struct {
	AvailableGPUCount int
	IsFiltered        bool
	FilteredStatus    Status
	Scores            []PluginScore
	TotalNodeScore    int
	GPUScores         map[string]*GPUScore
	TotalGPUScore     int
	FinalScore        int
	BestGPU           string
}

type GPUScore struct {
	UUID           string
	IsFiltered     bool
	FilteredStatus Status
	GPUScore       int
	PodCount       int
	IsSelected     bool
}

type PluginScore struct {
	PluginName string
	Score      int64
}

func NewPluginResult() *PluginResult {
	return &PluginResult{
		AvailableGPUCount: 0,
		IsFiltered:        false,
		FilteredStatus:    Status{},
		TotalNodeScore:    0,
		GPUScores:         make(map[string]*GPUScore),
		TotalGPUScore:     0,
		FinalScore:        0,
		BestGPU:           "",
	}
}

func (pr *PluginResult) InitPluginResult() {
	pr.AvailableGPUCount = 0
	pr.IsFiltered = false
	pr.FilteredStatus = Status{}
	pr.TotalNodeScore = 0
	for uuid, gpuscore := range pr.GPUScores {
		gpuscore.InitGPUScore(uuid)
	}
	pr.TotalGPUScore = 0
	pr.FinalScore = 0
	pr.BestGPU = ""
}

func NewGPUScore(uuid string) *GPUScore {
	return &GPUScore{
		UUID:           uuid,
		IsFiltered:     false,
		FilteredStatus: Status{},
		GPUScore:       0,
		PodCount:       0,
		IsSelected:     false,
	}
}

func (gs *GPUScore) InitGPUScore(uuid string) {
	gs.UUID = uuid
	gs.GPUScore = 0
	gs.FilteredStatus = Status{}
	gs.IsFiltered = false
	gs.IsSelected = false
}

type Code int

const (
	Success Code = iota
	Error
	Unschedulable
	UnschedulableAndUnresolvable
	Wait
	Skip
	Pending
)

var codes = []string{"Success", "Error", "Unschedulable", "UnschedulableAndUnresolvable", "Wait", "Skip", "Pending"}

func (c Code) String() string {
	return codes[c]
}

const (
	MaxNodeScore  int64 = 100
	MinNodeScore  int64 = 0
	MaxTotalScore int64 = math.MaxInt64
)

type Status struct {
	code    Code
	reasons []string
	err     error
	plugin  string
}

func (s *Status) WithError(err error) *Status {
	s.err = err
	return s
}

// Code returns code of the Status.
func (s *Status) Code() Code {
	if s == nil {
		return Success
	}
	return s.code
}

// Message returns a concatenated message on reasons of the Status.
func (s *Status) Message() string {
	if s == nil {
		return ""
	}
	return strings.Join(s.Reasons(), ", ")
}

// SetPlugin sets the given plugin name to s.plugin.
func (s *Status) SetPlugin(plugin string) {
	s.plugin = plugin
}

// WithPlugin sets the given plugin name to s.plugin,
// and returns the given status object.
func (s *Status) WithPlugin(plugin string) *Status {
	s.SetPlugin(plugin)
	return s
}

// Plugin returns the plugin name which caused this status.
func (s *Status) Plugin() string {
	return s.plugin
}

// Reasons returns reasons of the Status.
func (s *Status) Reasons() []string {
	if s.err != nil {
		return append([]string{s.err.Error()}, s.reasons...)
	}
	return s.reasons
}

// AppendReason appends given reason to the Status.
func (s *Status) AppendReason(reason string) {
	s.reasons = append(s.reasons, reason)
}

// IsSuccess returns true if and only if "Status" is nil or Code is "Success".
func (s *Status) IsSuccess() bool {
	return s.Code() == Success
}

// IsWait returns true if and only if "Status" is non-nil and its Code is "Wait".
func (s *Status) IsWait() bool {
	return s.Code() == Wait
}

// IsSkip returns true if and only if "Status" is non-nil and its Code is "Skip".
func (s *Status) IsSkip() bool {
	return s.Code() == Skip
}

// IsRejected returns true if "Status" is Unschedulable (Unschedulable, UnschedulableAndUnresolvable, or Pending).
func (s *Status) IsRejected() bool {
	code := s.Code()
	return code == Unschedulable || code == UnschedulableAndUnresolvable || code == Pending
}

// AsError returns nil if the status is a success, a wait or a skip; otherwise returns an "error" object
// with a concatenated message on reasons of the Status.
func (s *Status) AsError() error {
	if s.IsSuccess() || s.IsWait() || s.IsSkip() {
		return nil
	}
	if s.err != nil {
		return s.err
	}
	return errors.New(s.Message())
}

// Equal checks equality of two statuses. This is useful for testing with
// cmp.Equal.
func (s *Status) Equal(x *Status) bool {
	if s == nil || x == nil {
		return s.IsSuccess() && x.IsSuccess()
	}
	if s.code != x.code {
		return false
	}
	if !cmp.Equal(s.err, x.err, cmpopts.EquateErrors()) {
		return false
	}
	if !cmp.Equal(s.reasons, x.reasons) {
		return false
	}
	return cmp.Equal(s.plugin, x.plugin)
}

func (s *Status) String() string {
	return s.Message()
}

// NewStatus makes a Status out of the given arguments and returns its pointer.
func NewStatus(code Code, reasons ...string) *Status {
	s := &Status{
		code:    code,
		reasons: reasons,
	}
	return s
}

// AsStatus wraps an error in a Status.
func AsStatus(err error) *Status {
	if err == nil {
		return nil
	}
	return &Status{
		code: Error,
		err:  err,
	}
}
