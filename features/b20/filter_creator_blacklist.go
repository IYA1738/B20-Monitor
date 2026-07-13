package b20

import (
	"context"
	"encoding/json"
	"fmt"
	"token-discover-demo/chains/evm"
	"token-discover-demo/database"

	"github.com/ethereum/go-ethereum/common"
)

type CreatorBlacklistFilter struct {
	store *database.BlacklistStore
}

func NewCreatorBlacklistFilter(store *database.BlacklistStore) (*CreatorBlacklistFilter, error) {
	if store == nil {
		return nil, fmt.Errorf("black list store is nil")
	}

	return &CreatorBlacklistFilter{
		store: store,
	}, nil
}

func (f *CreatorBlacklistFilter) Name() string {
	return "b20_creator_blacklist"
}

func (f *CreatorBlacklistFilter) Filter(ctx context.Context, event evm.EventEnvelope) (evm.FilterResult, error) {
	if event.EventName != "b20_created" {
		return evm.Accept(), nil
	}

	var payload struct {
		Creator string `json:"creator"`
	}

	// 只要creator
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return evm.FilterResult{}, fmt.Errorf("decode b20 payload event_key=%s: %w", event.EventKey, err)
	}

	if !common.IsHexAddress(payload.Creator) {
		return evm.FilterResult{}, fmt.Errorf("invalid or missing b20 creator event_key=%s creator=%q", event.EventKey, payload.Creator)
	}

	creator := common.HexToAddress(payload.Creator)

	blocked, err := f.store.IsBlacklisted(
		ctx,
		event.ChainID,
		database.BlacklistScopeB20Creator,
		creator,
	)

	if err != nil {
		return evm.FilterResult{}, err
	}

	if blocked {
		return evm.Reject("b20 creator blacklisted: " + creator.Hex()), nil
	}

	return evm.Accept(), nil
}
