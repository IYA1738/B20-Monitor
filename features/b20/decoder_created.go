package b20

import (
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
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
}

type CreatedDecoder struct {
	factory common.Address

	topic0 common.Hash

	eventName string

	dataArgs abi.Arguments
}
