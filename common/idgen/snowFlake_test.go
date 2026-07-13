package idgen

import (
	"context"
	"testing"
	"time"
)

func TestGeneratorNextIDIncrementsSequenceWithinSameMillisecond(t *testing.T) {
	g, err := New(Config{
		WorkerID: 1,
	})
	if err != nil {
		t.Fatalf("new generator: %v", err)
	}

	now := time.UnixMilli(defaultEpochMillis + 1000)
	g.now = func() time.Time {
		return now
	}

	first, err := g.NextID(context.Background())
	if err != nil {
		t.Fatalf("first id: %v", err)
	}

	second, err := g.NextID(context.Background())
	if err != nil {
		t.Fatalf("second id: %v", err)
	}

	if first == second {
		t.Fatalf("expected unique ids, got %d twice", first)
	}

	firstParts := Parse(first, g.epochMillis)
	secondParts := Parse(second, g.epochMillis)

	if firstParts.Sequence != 0 {
		t.Fatalf("first sequence = %d, want 0", firstParts.Sequence)
	}

	if secondParts.Sequence != 1 {
		t.Fatalf("second sequence = %d, want 1", secondParts.Sequence)
	}
}
