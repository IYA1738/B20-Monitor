package idgen

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	defaultEpochMillis int64 = 1704067200000

	workerIDBits uint8 = 10
	sequenceBits uint8 = 12

	maxWorkerID int64 = -1 ^ (-1 << workerIDBits)
	sequenceMax int64 = -1 ^ (-1 << sequenceBits)

	workerIDShift  = sequenceBits
	timestampShift = workerIDBits + sequenceBits
)

type Config struct {
	WorkerID int64

	EpochMillis int64

	MaxClockBackward time.Duration
}

type Generator struct {
	mu sync.Mutex

	workerID    int64
	epochMillis int64
	lastMillis  int64

	sequence int64

	maxClockBackward time.Duration

	now func() time.Time
}

type Parts struct {
	ID       int64
	Time     time.Time
	WorkerID int64
	Sequence int64
}

func New(cfg Config) (*Generator, error) {
	if cfg.WorkerID < 0 || cfg.WorkerID > maxWorkerID {
		return nil, fmt.Errorf("worker id out of range: %d, allowed range: 0-%d", cfg.WorkerID, maxWorkerID)
	}

	if cfg.EpochMillis <= 0 {
		cfg.EpochMillis = defaultEpochMillis
	}

	if cfg.MaxClockBackward <= 0 {
		cfg.MaxClockBackward = 5 * time.Millisecond
	}

	return &Generator{
		workerID:         cfg.WorkerID,
		epochMillis:      cfg.EpochMillis,
		maxClockBackward: cfg.MaxClockBackward,
		now:              time.Now,
		lastMillis:       -1,
		sequence:         0, // sequence的用处是 同一个worker 在同一毫秒内生成多个ID的自增序号
	}, nil
}

func (g *Generator) NextID(ctx context.Context) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	currentMillis := g.currentMillis()

	if currentMillis < g.lastMillis {
		backward := time.Duration(g.lastMillis-currentMillis) * time.Millisecond
		if backward > g.maxClockBackward {
			return 0, fmt.Errorf("clock move backwards by %s", backward)
		}

		nextMillis, err := g.waitNextMillis(ctx, g.lastMillis)
		if err != nil {
			return 0, fmt.Errorf("wait clock recovery: %w", err)
		}
		currentMillis = nextMillis
	}

	if currentMillis == g.lastMillis {
		g.sequence++
		if g.sequence > sequenceMax {
			nextMillis, err := g.waitNextMillis(ctx, g.lastMillis)
			if err != nil {
				return 0, fmt.Errorf("wait next millisecond: %w", err)
			}
			currentMillis = nextMillis
			g.sequence = 0
		}
	} else {
		g.sequence = 0
	}

	g.lastMillis = currentMillis

	id := ((currentMillis - g.epochMillis) << int64(timestampShift)) | (g.workerID << int64(workerIDShift)) | g.sequence
	return id, nil
}

func Parse(id int64, epochMillis int64) Parts {
	if epochMillis <= 0 {
		epochMillis = defaultEpochMillis
	}

	timestampMillis := (id >> timestampShift) + epochMillis
	workerID := (id >> workerIDShift) & maxWorkerID
	sequence := id & sequenceMax

	return Parts{
		ID:       id,
		Time:     time.UnixMilli(timestampMillis),
		WorkerID: workerID,
		Sequence: sequence,
	}
}

func (g *Generator) currentMillis() int64 {
	return g.now().UnixMilli()
}

func (g *Generator) waitNextMillis(ctx context.Context, lastMillis int64) (int64, error) {
	for {
		currentMillis := g.currentMillis()
		if currentMillis > lastMillis {
			return currentMillis, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}
