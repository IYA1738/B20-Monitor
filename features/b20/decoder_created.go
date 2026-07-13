package b20

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"token-discover-demo/chains/evm"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

type CreatedDecoderConfig struct {
	Factory string `json:"factory"`

	Topic0 string `json:"topic0"`

	EventName string `json:"event_name"`
}

type CreatedPayload struct {
	Factory string `json:"factory"`

	Token string `json:"token"`

	Variant uint8 `json:"variant"`

	VariantName string `json:"variant_name"`

	Name string `json:"name"`

	Symbol string `json:"symbol"`

	Decimals uint8 `json:"decimals"`

	VariantParams string `json:"variant_params"`

	Creator string `json:"creator"`
}

type CreatedDecoder struct {
	factory common.Address

	topic0 common.Hash

	eventName string

	dataArgs abi.Arguments
}

func NewCreatedDecoderFromConfig(ctx context.Context, cfg evm.ComponentConfig) (evm.LogDecoder, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	var params CreatedDecoderConfig

	if err := evm.DecodeComponentParams(cfg, &params); err != nil {
		return nil, err
	}

	return NewCreatedDecoder(params)
}

func (d *CreatedDecoder) DecoderKeys() []evm.DecoderKey {
	return []evm.DecoderKey{
		evm.ExactDecoderKey(d.factory, d.topic0),
	}
}

func NewCreatedDecoder(cfg CreatedDecoderConfig) (*CreatedDecoder, error) {
	if cfg.Factory == "" {
		return nil, fmt.Errorf("b20 created decoder factory is empty")
	}

	if !common.IsHexAddress(cfg.Factory) {
		return nil, fmt.Errorf("b20 created decoder factory is not hex address: %s", cfg.Factory)
	}

	topic0, err := parseStrictHash(cfg.Topic0)

	if err != nil {
		return nil, fmt.Errorf("b20 created decoder topic0:%w", err)
	}

	dataArgs, err := buildCreatedDataArgs()

	if err != nil {
		return nil, err
	}

	eventName := strings.TrimSpace(cfg.EventName)
	if eventName == "" {
		eventName = "b20_created"
	}

	return &CreatedDecoder{
		factory:   common.HexToAddress(cfg.Factory),
		topic0:    topic0,
		eventName: eventName,
		dataArgs:  dataArgs,
	}, nil
}

func (d *CreatedDecoder) Decode(ctx context.Context, batch evm.ScanBatch, rawLog types.Log) (*evm.DecodedEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if rawLog.Address != d.factory {
		return nil, fmt.Errorf("unexpected B20Created address got=%s want=%s", rawLog.Address.Hex(), d.factory.Hex())
	}

	if len(rawLog.Topics) == 0 {
		return nil, fmt.Errorf("B20Created log has no topics")
	}

	if rawLog.Topics[0] != d.topic0 {
		return nil, fmt.Errorf("unexpected B20Created topic0 got=%s want=%s", rawLog.Topics[0].Hex(), d.topic0.Hex())
	}

	if len(rawLog.Topics) < 3 {
		return nil, fmt.Errorf("B20Created topics length too short: got=%d want>=3", len(rawLog.Topics))
	}

	token := topicToAddress(rawLog.Topics[1])

	variant, err := topicToUint8(rawLog.Topics[2])

	if err != nil {
		return nil, fmt.Errorf("decode B20Created variant: %w", err)
	}

	values, err := d.dataArgs.Unpack(rawLog.Data)

	if err != nil {
		return nil, fmt.Errorf("unpack B20Created data: %w", err)
	}

	if len(values) != 4 {
		return nil, fmt.Errorf("unexpected B20Created data values length got=%d want=4", len(values))
	}

	name, err := asString(values[0], "name")
	if err != nil {
		return nil, err
	}

	symbol, err := asString(values[1], "symbol")
	if err != nil {
		return nil, err
	}

	decimals, err := asUint8(values[2], "decimals")
	if err != nil {
		return nil, err
	}

	variantParams, err := asBytes(values[3], "variantParams")
	if err != nil {
		return nil, err
	}

	creator, ok := batch.TxSenders[rawLog.TxHash]

	if !ok || creator == (common.Address{}) {
		return nil, fmt.Errorf("missing tx sender tx=%s block=%d log_index=%d", rawLog.TxHash.Hex(), rawLog.BlockNumber, rawLog.Index)
	}
	payload := CreatedPayload{
		Factory:       d.factory.Hex(),
		Token:         token.Hex(),
		Variant:       variant,
		VariantName:   variantName(variant),
		Name:          name,
		Symbol:        symbol,
		Decimals:      decimals,
		VariantParams: hexutil.Encode(variantParams),
		Creator:       strings.ToLower(creator.Hex()),
	}

	return evm.NewDecodedEvent(d.eventName, payload)

}

func asString(value any, filed string) (string, error) {
	out, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("B20Created field=%s expected string got=%T", filed, value)
	}

	return out, nil
}

func asUint8(value any, field string) (uint8, error) {
	switch v := value.(type) {
	case uint8:
		return v, nil
	case uint64:
		if v > 255 {
			return 0, fmt.Errorf("B20Created field=%s uint8 overflow value=%d", field, v)
		}
		return uint8(v), nil

	case *big.Int:
		if v == nil {
			return 0, fmt.Errorf("B20Created field=%s is nil *big.Int", field)
		}
		if v.Sign() < 0 || v.Cmp(big.NewInt(255)) > 0 {
			return 0, fmt.Errorf("B20Created field=%s uint8 overflow value=%s", field, v.String())
		}
		return uint8(v.Uint64()), nil

	default:
		return 0, fmt.Errorf("B20Created field=%s expected uint8 got=%T", field, value)
	}
}

func asBytes(value any, field string) ([]byte, error) {
	out, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("B20Created field=%s expected []byte got=%T", field, value)
	}

	return out, nil
}

func variantName(variant uint8) string {
	switch variant {
	case 0:
		return "asset"
	default:
		return "unknown"
	}
}

func topicToUint8(topic common.Hash) (uint8, error) {
	n := topic.Big()

	if n.Sign() < 0 {
		return 0, fmt.Errorf("negative uint8 topic")
	}

	if n.Cmp(big.NewInt(255)) > 0 {
		return 0, fmt.Errorf("uint8 overflow value=%s", n.String())
	}
	return uint8(n.Uint64()), nil
}

func (d *CreatedDecoder) Name() string {
	return "b20_created"
}

func buildCreatedDataArgs() (abi.Arguments, error) {
	stringType, err := abi.NewType("string", "", nil)
	if err != nil {
		return nil, fmt.Errorf("Created abi string type:%w", err)
	}
	uint8Type, err := abi.NewType("uint8", "", nil)
	if err != nil {
		return nil, fmt.Errorf("Created abi uint8 type:%w", err)
	}
	bytesType, err := abi.NewType("bytes", "", nil)
	if err != nil {
		return nil, fmt.Errorf("Created abi bytes type:%w", err)
	}

	return abi.Arguments{
		{
			Name: "name",
			Type: stringType,
		},
		{
			Name: "symbol",
			Type: stringType,
		},
		{
			Name: "decimals",
			Type: uint8Type,
		},
		{
			Name: "variantParams",
			Type: bytesType,
		},
	}, nil
}

func parseStrictHash(raw string) (common.Hash, error) {
	raw = strings.TrimSpace(raw)

	if raw == "" {
		return common.Hash{}, fmt.Errorf("hash is empty")
	}

	if !strings.HasPrefix(raw, "0x") {
		return common.Hash{}, fmt.Errorf("hast must start with 0x")
	}

	// topic0必须是 0x + 64 一共66
	if len(raw) != 66 {
		return common.Hash{}, fmt.Errorf("hash length must be 66, got=%d", len(raw))
	}

	return common.HexToHash(raw), nil
}

func topicToAddress(topic common.Hash) common.Address {
	b := topic.Bytes()
	return common.BytesToAddress(b[12:])
}
