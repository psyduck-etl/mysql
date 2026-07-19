package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/psyduck-etl/sdk"
)

// groupingConfig is an embeddable config fragment that defines batching
// semantics for transformers. It supports two mutually exclusive strategies:
// - group-n: flush when buffer holds N items, with optional max-query-size
// - group-time: flush when a time window closes
//
// Either strategy is optional; if neither is set, the transformer runs
// unbatched. If both are set, bind() returns an error.
type groupingConfig struct {
	GroupN    *struct {
		Size         int `psy:"size"`
		MaxQuerySize int `psy:"max-query-size"`
	} `psy:"group-n"`
	GroupTime *struct{ Window string } `psy:"group-time"`
}

// groupingSpec returns the config fragment specs for the grouping options.
func groupingSpec() []*sdk.Spec {
	return []*sdk.Spec{
		{
			Name:        "group-n",
			Description: "flush when buffer holds this many items, or when upstream closes",
			Type:        sdk.TypeObject,
			Required:    false,
			Fields: []*sdk.Spec{
				{
					Name:        "size",
					Description: "number of items to accumulate before flushing",
					Type:        sdk.TypeInt,
					Required:    true,
				},
				{
					Name:        "max-query-size",
					Description: "for transformers that split large buffers into multiple queries, the maximum values per query (e.g., max size of a VALUES clause). Default 10,000. Set to 0 to disable splitting",
					Type:        sdk.TypeInt,
					Required:    false,
					Default:     10000,
				},
			},
		},
		{
			Name:        "group-time",
			Description: "flush when a time window closes (measured from first message in the window)",
			Type:        sdk.TypeObject,
			Required:    false,
			Fields: []*sdk.Spec{
				{
					Name:        "window",
					Description: "duration string in form {n}{h|m|s|ms}, e.g. \"500ms\", \"5s\", \"1m\"",
					Type:        sdk.TypeString,
					Required:    true,
				},
			},
		},
	}
}

// flusher is a small interface that strategies implement to define when
// to flush the current buffer.
type flusher interface {
	// shouldFlush examines the current buffer state and returns true if a
	// flush should occur. recvTime is the arrival time of a new message.
	shouldFlush(msg []byte, recvTime time.Time, bufLen int) bool
	// reset clears any internal state after a flush.
	reset(recvTime time.Time)
}

// countFlusher flushes when the buffer reaches a target size.
type countFlusher struct {
	size          int
	maxQuerySize  int // For query splitting (0 = no splitting)
}

func (c *countFlusher) shouldFlush(msg []byte, recvTime time.Time, bufLen int) bool {
	return bufLen >= c.size
}

func (c *countFlusher) reset(recvTime time.Time) {}

// MaxQuerySize returns the configured max query size, or 0 if not configured.
func (c *countFlusher) MaxQuerySize() int {
	return c.maxQuerySize
}

// timeFlusher flushes when a message arrives outside the current window.
type timeFlusher struct {
	window    time.Duration
	baseline  time.Time
	hasBaseline bool
}

func (t *timeFlusher) shouldFlush(msg []byte, recvTime time.Time, bufLen int) bool {
	if !t.hasBaseline {
		return false // first message of a window, don't flush yet
	}
	return recvTime.Sub(t.baseline) >= t.window
}

func (t *timeFlusher) reset(recvTime time.Time) {
	t.baseline = recvTime
	t.hasBaseline = true
}

// bind validates the grouping config and returns a flusher strategy.
// Exactly zero or one of GroupN/GroupTime must be set.
func (g *groupingConfig) bind() (flusher, error) {
	nSet := g.GroupN != nil
	tSet := g.GroupTime != nil

	if !nSet && !tSet {
		return nil, nil // no grouping
	}

	if nSet && tSet {
		return nil, fmt.Errorf("mysql.filter grouping: specify exactly one of group-n or group-time, not both")
	}

	if nSet {
		if g.GroupN.Size <= 0 {
			return nil, fmt.Errorf("mysql.filter group-n: size must be > 0, got %d", g.GroupN.Size)
		}
		maxQuerySize := g.GroupN.MaxQuerySize
		if maxQuerySize < 0 {
			return nil, fmt.Errorf("mysql.filter group-n: max-query-size must be >= 0, got %d", maxQuerySize)
		}
		// Default to 10,000 if not specified and maxQuerySize is 0
		if maxQuerySize == 0 {
			maxQuerySize = 10000
		}
		return &countFlusher{size: g.GroupN.Size, maxQuerySize: maxQuerySize}, nil
	}

	// tSet is true
	dur, err := parseDuration(g.GroupTime.Window)
	if err != nil {
		return nil, fmt.Errorf("mysql.filter group-time: %w", err)
	}
	if dur <= 0 {
		return nil, fmt.Errorf("mysql.filter group-time: window must be > 0, got %v", dur)
	}
	return &timeFlusher{window: dur}, nil
}

// parseDuration parses a psyduck-style duration string: {n}{h|m|s|ms}
// Examples: "500ms", "5s", "1m", "2h"
func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration string")
	}

	// Scan from the start to find where digits end and the unit begins.
	// Examples: "500ms" -> numStr="500", unit="ms"
	//           "5s" -> numStr="5", unit="s"
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}

	if i == 0 {
		return 0, fmt.Errorf("duration string has no number: %q", s)
	}

	if i == len(s) {
		return 0, fmt.Errorf("duration string has no unit: %q", s)
	}

	numStr := s[:i]
	unit := s[i:]

	// Parse the numeric part
	num, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration number: %q", numStr)
	}

	if num <= 0 {
		return 0, fmt.Errorf("duration number must be positive, got %d", num)
	}

	// Map the unit to a time.Duration multiplier
	switch unit {
	case "ms":
		return time.Duration(num) * time.Millisecond, nil
	case "s":
		return time.Duration(num) * time.Second, nil
	case "m":
		return time.Duration(num) * time.Minute, nil
	case "h":
		return time.Duration(num) * time.Hour, nil
	default:
		return 0, fmt.Errorf("unknown duration unit: %q (want h|m|s|ms)", unit)
	}
}
