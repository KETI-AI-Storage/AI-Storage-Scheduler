package framework

import (
	"errors"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

var (
	// ErrNotFound is the not found error message.
	ErrNotFound = errors.New("not found")
)

type StateData interface {
	// Clone is an interface to make a copy of StateData. For performance reasons,
	// clone should make shallow copies for members (e.g., slices or maps) that are not
	// impacted by PreFilter's optional AddPod/RemovePod methods.
	Clone() StateData
}

type StateKey string

type CycleState struct { // 각 플러그인의 결과를 저장
	// storage is keyed with StateKey, and valued with StateData.
	storage sync.Map
	// if recordPluginMetrics is true, metrics.PluginExecutionDuration will be recorded for this cycle.
	recordPluginMetrics bool
	// SkipFilterPlugins are plugins that will be skipped in the Filter extension point.
	SkipFilterPlugins sets.Set[string]
	// SkipScorePlugins are plugins that will be skipped in the Score extension point.
	SkipScorePlugins sets.Set[string]
}

func NewCycleState() *CycleState {
	return &CycleState{}
}

func (c *CycleState) ShouldRecordPluginMetrics() bool {
	if c == nil {
		return false
	}
	return c.recordPluginMetrics
}

// SetRecordPluginMetrics sets recordPluginMetrics to the given value.
func (c *CycleState) SetRecordPluginMetrics(flag bool) {
	if c == nil {
		return
	}
	c.recordPluginMetrics = flag
}

// Clone creates a copy of CycleState and returns its pointer. Clone returns
// nil if the context being cloned is nil.
func (c *CycleState) Clone() *CycleState {
	if c == nil {
		return nil
	}
	copy := NewCycleState()
	// Safe copy storage in case of overwriting.
	c.storage.Range(func(k, v interface{}) bool {
		copy.storage.Store(k, v.(StateData).Clone())
		return true
	})
	// The below are not mutated, so we don't have to safe copy.
	copy.recordPluginMetrics = c.recordPluginMetrics
	copy.SkipFilterPlugins = c.SkipFilterPlugins
	copy.SkipScorePlugins = c.SkipScorePlugins

	return copy
}

// Read retrieves data with the given "key" from CycleState. If the key is not
// present, ErrNotFound is returned.
//
// See CycleState for notes on concurrency.
func (c *CycleState) Read(key StateKey) (StateData, error) {
	if v, ok := c.storage.Load(key); ok {
		return v.(StateData), nil
	}
	return nil, ErrNotFound
}

// Write stores the given "val" in CycleState with the given "key".
//
// See CycleState for notes on concurrency.
func (c *CycleState) Write(key StateKey, val StateData) {
	c.storage.Store(key, val)
}

// Delete deletes data with the given key from CycleState.
//
// See CycleState for notes on concurrency.
func (c *CycleState) Delete(key StateKey) {
	c.storage.Delete(key)
}
