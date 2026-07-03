package evm

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
)

type Cursor struct {
	Key string

	ChainID   uint64
	ChainName string
	Network   string
	Monitor   string

	NextBlock   uint64
	SafeBlock   uint64
	LatestBlock uint64
}

type CursorStore interface {
	GetCursor(ctx context.Context, key string) (*Cursor, bool, error)
	SaveCursor(ctx context.Context, cursor Cursor) error
}

type LogSource interface {
	Name() string
	BuildQueries(ctx context.Context, fromBlock uint64, toBlock uint64) ([]ethereum.FilterQuery, error)
}

type LogHandler interface {
	HandleLogs(ctx context.Context, batch ScanBatch, logs []types.Log) error
}

type ScannerConfig struct {
	ChainID   uint64
	ChainName string
	Network   string

	StartBlock    uint64
	Confirmations uint64
	ChunkSize     uint64

	FetchWorkers      int
	MaxInflightChunks int // 每轮最多扫多少chunk
	ErrorBackoff      time.Duration

	PollInterval time.Duration
}

type Scanner struct {
	cfg ScannerConfig

	rpc     *RPCClient
	source  LogSource
	cursor  CursorStore
	handler LogHandler
}

type ScanBatch struct {
	ChainID   uint64
	ChainName string
	Network   string

	Monitor string

	FromBlock   uint64
	ToBlock     uint64
	SafeBlock   uint64
	LatestBlock uint64

	LogCount int
}
type chunkJob struct {
	Index     int
	FromBlock uint64
	ToBlock   uint64
}

type chunkResult struct {
	Job chunkJob

	LogCount int
	Err      error

	StartedAt  time.Time
	FinishedAt time.Time
}

func NewScanner(
	cfg ScannerConfig,
	rpc *RPCClient,
	source LogSource,
	cursor CursorStore,
	handler LogHandler,
) (*Scanner, error) {
	applyScannerDefault(&cfg)

	if cfg.ChainID == 0 {
		return nil, fmt.Errorf("scanner chain id is required")
	}

	if cfg.ChainName == "" {
		return nil, fmt.Errorf("scanner chain name is required")
	}

	if cfg.Network == "" {
		return nil, fmt.Errorf("scanner network is required")
	}

	if rpc == nil {
		return nil, fmt.Errorf("scanner rpc client is nil")
	}

	if source == nil {
		return nil, fmt.Errorf("scanner log source is nil")
	}

	if cursor == nil {
		return nil, fmt.Errorf("scanner cursor store is nil")
	}

	if handler == nil {
		return nil, fmt.Errorf("scanner log handler is nil")
	}

	return &Scanner{
		cfg:     cfg,
		rpc:     rpc,
		source:  source,
		cursor:  cursor,
		handler: handler,
	}, nil
}

func applyScannerDefault(cfg *ScannerConfig) {
	if cfg.Confirmations == 0 {
		cfg.Confirmations = 3
	}

	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 500
	}

	if cfg.FetchWorkers <= 0 {
		cfg.FetchWorkers = 10
	}

	if cfg.MaxInflightChunks <= 0 {
		cfg.MaxInflightChunks = cfg.FetchWorkers * 2
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 3 * time.Second
	}

	if cfg.ErrorBackoff <= 0 {
		cfg.ErrorBackoff = 3 * time.Second
	}
}

func (s *Scanner) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("Scanner is nil")
	}

	log.Printf(
		"[evm-scanner] started chain=%s network=%s chain_id=%d monitor=%s start_block=%d confirmations=%d chunk_size=%d fetch_workers=%d max_inflight_chunks=%d",
		s.cfg.ChainName,
		s.cfg.Network,
		s.cfg.ChainID,
		s.source.Name(),
		s.cfg.StartBlock,
		s.cfg.Confirmations,
		s.cfg.ChunkSize,
		s.cfg.FetchWorkers,
		s.cfg.MaxInflightChunks,
	)

	for {
		progressed, err := s.scanOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			log.Printf(
				"[evm-scanner] scan failed chain=%s network=%s monitor=%s progressed=%v err=%v",
				s.cfg.ChainName,
				s.cfg.Network,
				s.source.Name(),
				progressed,
				err,
			)

			if !progressed {
				if err := sleepContext(ctx, s.cfg.ErrorBackoff); err != nil {
					return err
				}
			}
			continue
		}

		if progressed {
			continue
		}

		// 已追上最新块就等一下
		if err := sleepContext(ctx, s.cfg.PollInterval); err != nil {
			return err
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-timer.C:
		return nil
	}
}

func (s *Scanner) scanOnce(ctx context.Context) (bool, error) {
	latestBlock, err := s.rpc.BlockNumber(ctx)
	if err != nil {
		return false, fmt.Errorf("get latest block:%w", err)
	}

	safeBlock, ok := s.safeBlock(latestBlock)

	if !ok {
		log.Printf(
			"[evm-scanner] waiting confirmations chain=%s network=%s monitor=%s latest=%d confirmations=%d",
			s.cfg.ChainName,
			s.cfg.Network,
			s.source.Name(),
			latestBlock,
			s.cfg.Confirmations,
		)
		return false, nil
	}

	cursorKey := s.cursorKey()

	cursor, exists, err := s.cursor.GetCursor(ctx, cursorKey)
	if err != nil {
		return false, fmt.Errorf("get cursor key = %s: %w", cursorKey, err)
	}

	nextBlock := s.cfg.StartBlock

	if exists && cursor != nil {
		nextBlock = cursor.NextBlock
	}

	// 扫到安全区块了，新的区块确认数太少， 先不扫
	if nextBlock > safeBlock {
		log.Printf(
			"[evm-scanner] caught up chain=%s network=%s monitor=%s next=%d safe=%d latest=%d",
			s.cfg.ChainName,
			s.cfg.Network,
			s.source.Name(),
			nextBlock,
			safeBlock,
			latestBlock,
		)
		return false, nil
	}

	chunks := s.planChunks(nextBlock, safeBlock)

	if len(chunks) == 0 {
		return false, nil
	}

	results := s.runChunks(ctx, chunks, safeBlock, latestBlock)

	advancedNextBlock, committedChunks, committedLogs, firstErr := s.computeCommitPoint(nextBlock, chunks, results)

	progressed := advancedNextBlock > cursor.NextBlock

	if progressed {
		nextCursor := Cursor{
			Key:         cursorKey,
			ChainID:     s.cfg.ChainID,
			ChainName:   s.cfg.ChainName,
			Network:     s.cfg.Network,
			Monitor:     s.source.Name(),
			NextBlock:   advancedNextBlock,
			SafeBlock:   safeBlock,
			LatestBlock: latestBlock,
		}

		if err := s.cursor.SaveCursor(ctx, nextCursor); err != nil {
			return false, fmt.Errorf("save cursor key=%s next=%d: %w", cursorKey, advancedNextBlock, err)
		}
	}

	log.Printf(
		"[evm-scanner] round done chain=%s network=%s monitor=%s planned_chunks=%d committed_chunks=%d committed_logs=%d next=%d safe=%d latest=%d progressed=%v",
		s.cfg.ChainName,
		s.cfg.Network,
		s.source.Name(),
		len(chunks),
		committedChunks,
		committedLogs,
		advancedNextBlock,
		safeBlock,
		latestBlock,
		progressed,
	)

	if firstErr != nil {
		return progressed, firstErr
	}

	return progressed, nil
}

func (s *Scanner) runChunks(ctx context.Context, chunks []chunkJob, safeBlock uint64, latestBlock uint64) map[uint64]chunkResult {
	results := make(chan chunkResult, len(chunks))
	jobs := make(chan chunkJob)

	workerCount := minInt(s.cfg.FetchWorkers, len(chunks))

	var wg sync.WaitGroup

	for workerID := 0; workerID < workerCount; workerID++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			for job := range jobs {
				result := s.processChunk(ctx, workerID, job, safeBlock, latestBlock)
				results <- result
			}
		}(workerID)
	}
	go func() {
		defer close(jobs)

		for _, job := range chunks {
			select {
			case jobs <- job:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	resultMap := make(map[uint64]chunkResult, len(chunks))

	for result := range results {
		resultMap[result.Job.FromBlock] = result
	}
	return resultMap
}

func minInt(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}

func (s *Scanner) processChunk(ctx context.Context, workerID int, job chunkJob, safeBlock uint64, latestBlock uint64) chunkResult {
	startedAt := time.Now()

	result := chunkResult{
		Job:       job,
		StartedAt: startedAt,
	}

	queries, err := s.source.BuildQueries(ctx, job.FromBlock, job.ToBlock)

	if err != nil {
		result.Err = fmt.Errorf("worker=%d build queries from=%d to=%d: %w", workerID, job.FromBlock, job.ToBlock, err)
		result.FinishedAt = time.Now()
		return result
	}

	var allLogs []types.Log

	for queryIndex, query := range queries {
		query.FromBlock = new(big.Int).SetUint64(job.FromBlock)
		query.ToBlock = new(big.Int).SetUint64(job.ToBlock)

		logs, err := s.rpc.FilterLogs(ctx, query)

		if err != nil {
			result.Err = fmt.Errorf(
				"worker=%d query=%d filter logs from=%d to=%d: %w",
				workerID,
				queryIndex,
				job.FromBlock,
				job.ToBlock,
				err,
			)
			result.FinishedAt = time.Now()
			return result
		}
		allLogs = append(allLogs, logs...)
	}

	batch := ScanBatch{
		ChainID:     s.cfg.ChainID,
		ChainName:   s.cfg.ChainName,
		Network:     s.cfg.Network,
		Monitor:     s.source.Name(),
		FromBlock:   job.FromBlock,
		ToBlock:     job.ToBlock,
		SafeBlock:   safeBlock,
		LatestBlock: latestBlock,
		LogCount:    len(allLogs),
	}

	if err := s.handler.HandleLogs(ctx, batch, allLogs); err != nil {
		result.Err = fmt.Errorf("worker=%d handle logs from=%d to=%d count=%d: %w", workerID, job.FromBlock, job.ToBlock, len(allLogs), err)
		result.LogCount = len(allLogs)
		result.FinishedAt = time.Now()
		return result
	}

	result.LogCount = len(allLogs)
	result.FinishedAt = time.Now()

	log.Printf(
		"[evm-scanner] chunk done chain=%s network=%s monitor=%s worker=%d from=%d to=%d logs=%d duration=%s",
		s.cfg.ChainName,
		s.cfg.Network,
		s.source.Name(),
		workerID,
		job.FromBlock,
		job.ToBlock,
		len(allLogs),
		result.FinishedAt.Sub(result.StartedAt),
	)

	return result
}

func (s *Scanner) planChunks(nextBlock uint64, safeBlock uint64) []chunkJob {
	chunks := make([]chunkJob, 0, s.cfg.MaxInflightChunks)

	fromBlock := nextBlock

	for len(chunks) < s.cfg.MaxInflightChunks && fromBlock <= safeBlock {
		toBlock := s.nextToBlock(fromBlock, safeBlock)

		chunks = append(chunks, chunkJob{
			Index:     len(chunks),
			FromBlock: fromBlock,
			ToBlock:   toBlock,
		})

		if toBlock == safeBlock {
			break
		}

		fromBlock = toBlock + 1
	}
	return chunks
}

func (s *Scanner) computeCommitPoint(
	nextBlock uint64,
	chunks []chunkJob,
	results map[uint64]chunkResult,
) (advancedNextBlock uint64, committedChunks int, committedLogs int, firstErr error) {
	advancedNextBlock = nextBlock

	for _, chunk := range chunks {
		result, ok := results[chunk.FromBlock]
		if !ok {
			firstErr = fmt.Errorf("missing chunk result from=%d to=%d", chunk.FromBlock, chunk.ToBlock)
			return advancedNextBlock, committedChunks, committedLogs, firstErr
		}

		if result.Err != nil {
			firstErr = result.Err
			return advancedNextBlock, committedChunks, committedLogs, firstErr
		}

		advancedNextBlock = result.Job.ToBlock + 1
		committedChunks++
		committedLogs += result.LogCount
	}
	return advancedNextBlock, committedChunks, committedLogs, nil
}

func (s *Scanner) nextToBlock(fromBlock uint64, safeBlock uint64) uint64 {
	toBlock := fromBlock + s.cfg.ChunkSize - 1

	if toBlock > safeBlock {
		return safeBlock
	}

	return toBlock
}

func (s *Scanner) cursorKey() string {
	return fmt.Sprintf(
		"%d:%s:%s",
		s.cfg.ChainID,
		s.cfg.Network,
		s.source.Name(),
	)
}

func (s *Scanner) safeBlock(latestBlock uint64) (uint64, bool) {
	if latestBlock <= s.cfg.Confirmations {
		return 0, false
	}
	return latestBlock - s.cfg.Confirmations, true
}
