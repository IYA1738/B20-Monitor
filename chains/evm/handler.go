package evm

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

type rawLogJSON struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	BlockNumber uint64   `json:"block_number"`
	BlockHash   string   `json:"block_hash"`
	TxHash      string   `json:"tx_hash"`
	TxIndex     uint     `json:"tx_index"`
	LogIndex    uint     `json:"log_index"`
	Removed     bool     `json:"removed"`
}

type DecodedEvent struct {
	EventName string
	Payload   json.RawMessage
}

func NewDecodedEvent(eventName string, payload any) (*DecodedEvent, error) {
	if eventName == "" {
		return nil, fmt.Errorf("decoded event name is empty")
	}

	if payload == nil {
		return nil, fmt.Errorf("decoded event payload is nil")
	}

	raw, ok := payload.(json.RawMessage)

	if ok {
		if !json.Valid(raw) {
			return nil, fmt.Errorf("decoded raw payload is not valid json")
		}

		return &DecodedEvent{
			EventName: eventName,
			Payload:   raw,
		}, nil
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal decoded payload: %w", err)
	}

	return &DecodedEvent{
		EventName: eventName,
		Payload:   body,
	}, nil
}

// 系统内部统一事件格式
type EventEnvelope struct {

	// EventKey = tx_hash + log_index 去重 因为特定的tx_hash的特定log_index本身就是唯一的一个事件
	EventKey string

	ChainID   uint64
	ChainName string
	Network   string
	Monitor   string

	EventName   string
	BlockNumber uint64
	BlockHash   common.Hash // 保存一下block hash，如果发生了变化那说明链重组了，从这块重新扫

	TxHash  common.Hash
	TxIndex uint

	LogIndex uint

	ContractAddress common.Address // 哪个合约发的事件
	Topic0          common.Hash

	Payload json.RawMessage

	RawLog json.RawMessage

	Removed bool // 是否因为链重组移除

	ObservedAt time.Time // 监听到事件的时间
}

type LogDecoder interface {
	Name() string

	// 返回处理这个Address和Topic0的decoder
	DecoderKeys() []DecoderKey

	Decode(ctx context.Context, batch ScanBatch, rawLog types.Log) (*DecodedEvent, error)
}

type FilterResult struct {
	// 事件是否通过过滤
	Accepted bool
	// 没有通过时的原因
	Reason string
}

func Accept() FilterResult {
	return FilterResult{
		Accepted: true,
	}
}

func Reject(reason string) FilterResult {
	return FilterResult{
		Accepted: false,
		Reason:   reason,
	}
}

// 合约地址和Topic0就可以锁定唯一的decoder，用这个来避免遍历decoders来找对应的decoder
type DecoderKey struct {
	Address common.Address
	Topic0  common.Hash

	// true表示只筛Topic0 不筛Address
	MatchAnyAddress bool
}

func ExactDecoderKey(address common.Address, topic0 common.Hash) DecoderKey {
	return DecoderKey{
		Address: address,
		Topic0:  topic0,
	}
}

func TopicDecoderKey(topic0 common.Hash) DecoderKey {
	return DecoderKey{
		Topic0:          topic0,
		MatchAnyAddress: true,
	}
}

type EventFilter interface {
	Name() string
	// Filter判断事件有没有通过 并且去用上面俩函数返回FilterResult
	Filter(ctx context.Context, event EventEnvelope) (FilterResult, error)
}

type EventSink interface {
	// 给数据库保存事件
	SaveEvents(ctx context.Context, events []EventEnvelope) error
}

type PipelineHandlerConfig struct {

	// false的时候如果log没有对应的decoder跳过log继续运行， true的时候无法解码抛出异常
	FailOnUnmatched bool

	// false的时候如果decoder报错直接返回error暂停， true的时候只打日志跳过这个log继续运行``
	IgnoreDecodeError bool
}

type exactDecoderKey struct {
	Address common.Address
	Topic0  common.Hash
}

// 把scanner.go扫的raw log拿进来handler处理
type PipelineHandler struct {
	cfg PipelineHandlerConfig

	// 同时查事件和地址
	exactDecoders map[exactDecoderKey]LogDecoder

	// 查事件
	topicDecoders map[common.Hash]LogDecoder

	filters []EventFilter // 一组业务过滤

	sink EventSink

	now func() time.Time
}

func NewPipelineHandler(
	cfg PipelineHandlerConfig,
	decoders []LogDecoder,
	filters []EventFilter,
	sink EventSink) (*PipelineHandler, error) {
	if sink == nil {
		return nil, fmt.Errorf("sink is nil")
	}

	exactDecoders, topicDecoders, err := buildDecoderIndex(decoders)
	if err != nil {
		return nil, err
	}

	return &PipelineHandler{
		cfg:           cfg,
		exactDecoders: exactDecoders,
		topicDecoders: topicDecoders,
		filters:       append([]EventFilter(nil), filters...),
		sink:          sink,
		now:           time.Now,
	}, nil
}

func buildDecoderIndex(decoders []LogDecoder) (map[exactDecoderKey]LogDecoder, map[common.Hash]LogDecoder, error) {
	exact := make(map[exactDecoderKey]LogDecoder)
	topicOnly := make(map[common.Hash]LogDecoder)

	for _, decoder := range decoders {
		if decoder == nil {
			continue
		}

		name := decoder.Name()
		if name == "" {
			return nil, nil, fmt.Errorf("decoder name is empty")
		}

		keys := decoder.DecoderKeys()

		if len(keys) == 0 {
			return nil, nil, fmt.Errorf("decoder=%s has no decoder keys", name)
		}

		for keyIndex, key := range keys {
			if key.Topic0 == (common.Hash{}) {
				return nil, nil, fmt.Errorf("decoder=%s key[%d] has empty topic0", name, keyIndex)
			}

			if key.MatchAnyAddress {
				if old := topicOnly[key.Topic0]; old != nil {
					return nil, nil, fmt.Errorf(
						"duplicate topic decoder topic0=%s old=%s new=%s",
						key.Topic0.Hex(),
						old.Name(),
						name,
					)
				}
				topicOnly[key.Topic0] = decoder
				continue
			}

			if key.Address == (common.Address{}) {
				return nil, nil, fmt.Errorf("decoder=%s key[%d] has empty address", name, keyIndex)
			}

			exactKey := exactDecoderKey{
				Address: key.Address,
				Topic0:  key.Topic0,
			}

			if old := exact[exactKey]; old != nil {
				return nil, nil, fmt.Errorf(
					"duplicate exact decoder address=%s topic0=%s old=%s new=%s",
					key.Address.Hex(),
					key.Topic0.Hex(),
					old.Name(),
					name,
				)
			}

			exact[exactKey] = decoder
		}
	}
	return exact, topicOnly, nil
}

func (h *PipelineHandler) HandleLogs(ctx context.Context, batch ScanBatch, logs []types.Log) error {
	if h == nil {
		return fmt.Errorf("Pipelin handler is nil")
	}

	if len(logs) == 0 {
		return nil
	}

	events := make([]EventEnvelope, 0, len(logs))

	var unmatched int
	var decodeIgnored int
	var filtered int

	for i := range logs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rawLog := logs[i]

		decoder := h.findDecoder(rawLog)

		if decoder == nil {
			unmatched++

			if h.cfg.FailOnUnmatched {
				return fmt.Errorf(
					"no decoder matched log chain=%s network=%s monitor=%s block=%d tx=%s log_index=%d address=%s topic0=%s",
					batch.ChainName,
					batch.Network,
					batch.Monitor,
					rawLog.BlockNumber,
					rawLog.TxHash.Hex(),
					rawLog.Index,
					rawLog.Address.Hex(),
					topic0Hex(rawLog),
				)
			}

			continue
		}

		decoded, err := decoder.Decode(ctx, batch, rawLog)

		if err != nil {
			if !h.cfg.IgnoreDecodeError {
				return fmt.Errorf(
					"decode log failed decoder=%s chain=%s network=%s monitor=%s block=%d tx=%s log_index=%d: %w",
					decoder.Name(),
					batch.ChainName,
					batch.Network,
					batch.Monitor,
					rawLog.BlockNumber,
					rawLog.TxHash.Hex(),
					rawLog.Index,
					err,
				)
			}

			decodeIgnored++

			log.Printf(
				"[evm-handler] decode failed but ignored decoder=%s chain=%s network=%s monitor=%s block=%d tx=%s log_index=%d err=%v",
				decoder.Name(),
				batch.ChainName,
				batch.Network,
				batch.Monitor,
				rawLog.BlockNumber,
				rawLog.TxHash.Hex(),
				rawLog.Index,
				err,
			)

			continue
		}

		event, err := h.buildEvent(batch, rawLog, decoded)

		if err != nil {
			return fmt.Errorf(
				"build event failed chain=%s network=%s monitor=%s block=%d tx=%s log_index=%d: %w",
				batch.ChainName,
				batch.Network,
				batch.Monitor,
				rawLog.BlockNumber,
				rawLog.TxHash.Hex(),
				rawLog.Index,
				err,
			)
		}

		filterResult, err := h.applyFilters(ctx, event)
		if err != nil {
			return fmt.Errorf(
				"filter event failed event_key=%s event=%s: %w",
				event.EventKey,
				event.EventName,
				err,
			)
		}

		if !filterResult.Accepted {
			filtered++
			log.Printf(
				"[evm-handler] event filtered event_key=%s event=%s reason=%s",
				event.EventKey,
				event.EventName,
				filterResult.Reason,
			)

			continue
		}

		events = append(events, event)
	}

	if len(events) == 0 {
		log.Printf(
			"[evm-handler] no accepted events chain=%s network=%s monitor=%s from=%d to=%d raw_logs=%d unmatched=%d decode_ignored=%d filtered=%d",
			batch.ChainName,
			batch.Network,
			batch.Monitor,
			batch.FromBlock,
			batch.ToBlock,
			len(logs),
			unmatched,
			decodeIgnored,
			filtered,
		)

		return nil
	}

	if err := h.sink.SaveEvents(ctx, events); err != nil {
		return fmt.Errorf(
			"save events chain=%s network=%s monitor=%s from=%d to=%d events=%d: %w",
			batch.ChainName,
			batch.Network,
			batch.Monitor,
			batch.FromBlock,
			batch.ToBlock,
			len(events),
			err,
		)
	}

	log.Printf(
		"[evm-handler] logs handled chain=%s network=%s monitor=%s from=%d to=%d raw_logs=%d accepted=%d unmatched=%d decode_ignored=%d filtered=%d",
		batch.ChainName,
		batch.Network,
		batch.Monitor,
		batch.FromBlock,
		batch.ToBlock,
		len(logs),
		len(events),
		unmatched,
		decodeIgnored,
		filtered,
	)

	return nil

}

func (h *PipelineHandler) applyFilters(ctx context.Context, event EventEnvelope) (FilterResult, error) {
	for _, filter := range h.filters {
		if filter == nil {
			continue
		}

		result, err := filter.Filter(ctx, event)
		if err != nil {
			return FilterResult{}, fmt.Errorf("filter=%s: %w", filter.Name(), err)
		}

		if !result.Accepted {
			if result.Reason == "" {
				result.Reason = filter.Name()
			}

			return result, nil
		}
	}
	return Accept(), nil
}

func (h *PipelineHandler) buildEvent(batch ScanBatch, rawLog types.Log, decoded *DecodedEvent) (EventEnvelope, error) {
	if decoded == nil {
		return EventEnvelope{}, fmt.Errorf("decoded event is nil")
	}

	if decoded.EventName == "" {
		return EventEnvelope{}, fmt.Errorf("decoded event name is empty")
	}

	if len(decoded.Payload) == 0 {
		return EventEnvelope{}, fmt.Errorf("decoded payload is empty")
	}

	if !json.Valid(decoded.Payload) {
		return EventEnvelope{}, fmt.Errorf("decoded payload is not valid json")
	}

	rawJson, err := marshalRawLog(rawLog)

	if err != nil {
		return EventEnvelope{}, err
	}

	observedAt := time.Now()
	if h.now != nil {
		observedAt = h.now()
	}

	return EventEnvelope{
		EventKey: BuildEventKey(batch.ChainID, rawLog.TxHash, rawLog.Index),

		ChainID:   batch.ChainID,
		ChainName: batch.ChainName,
		Network:   batch.Network,
		Monitor:   batch.Monitor,

		EventName: decoded.EventName,

		BlockNumber: rawLog.BlockNumber,
		BlockHash:   rawLog.BlockHash,

		TxHash:  rawLog.TxHash,
		TxIndex: rawLog.TxIndex,

		LogIndex: rawLog.Index,

		ContractAddress: rawLog.Address,
		Topic0:          topic0(rawLog),

		Payload: decoded.Payload,
		RawLog:  rawJson,

		Removed:    rawLog.Removed,
		ObservedAt: observedAt,
	}, nil
}

func BuildEventKey(chainID uint64, txHash common.Hash, logIndex uint) string {
	return fmt.Sprintf("%d:%s:%d", chainID, txHash.Hex(), logIndex)
}

func marshalRawLog(rawLog types.Log) (json.RawMessage, error) {
	topics := make([]string, 0, len(rawLog.Topics))

	for _, topic := range rawLog.Topics {
		topics = append(topics, topic.Hex())
	}

	body := rawLogJSON{
		Address:     rawLog.Address.Hex(),
		Topics:      topics,
		Data:        hexutil.Encode(rawLog.Data),
		BlockNumber: rawLog.BlockNumber,
		BlockHash:   rawLog.BlockHash.Hex(),
		TxHash:      rawLog.TxHash.Hex(),
		TxIndex:     rawLog.TxIndex,
		LogIndex:    rawLog.Index,
		Removed:     rawLog.Removed,
	}

	out, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal raw log:%w", err)
	}

	return out, nil
}

func topic0(rawLog types.Log) common.Hash {
	if len(rawLog.Topics) == 0 {
		return common.Hash{}
	}
	return rawLog.Topics[0]
}

func topic0Hex(rawLog types.Log) string {
	if len(rawLog.Topics) == 0 {
		return ""
	}
	return rawLog.Topics[0].Hex()
}

func (h *PipelineHandler) findDecoder(rawLog types.Log) LogDecoder {
	if len(rawLog.Topics) == 0 {
		return nil
	}

	topic0 := rawLog.Topics[0]

	if decoder := h.exactDecoders[exactDecoderKey{
		Address: rawLog.Address,
		Topic0:  topic0,
	}]; decoder != nil {
		return decoder
	}

	if decoder := h.topicDecoders[topic0]; decoder != nil {
		return decoder
	}
	return nil
}

// 先占位
type NoopEventSink struct{}

func (NoopEventSink) SaveEvents(ctx context.Context, events []EventEnvelope) error {
	return nil
}
