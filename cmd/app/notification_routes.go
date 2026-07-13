package app

import (
	"context"

	"token-discover-demo/chains/evm"
	"token-discover-demo/configs"
	"token-discover-demo/database"
)

type staticNotificationRoutePlanner struct {
	routes []database.NotificationRoute
}

func newNotificationRoutePlanner(cfg configs.NotifierConfig) database.NotificationRoutePlanner {
	routes := make([]database.NotificationRoute, 0, 1)

	if cfg.Telegram.Enabled {
		routes = append(routes, database.NotificationRoute{
			Channel:    database.NotificationChannelTelegram,
			Target:     cfg.Telegram.ChatID,
			MaxRetries: 10,
		})
	}

	if len(routes) == 0 {
		return nil
	}

	return &staticNotificationRoutePlanner{
		routes: routes,
	}
}

func (p *staticNotificationRoutePlanner) RoutesForEvent(ctx context.Context, event evm.EventEnvelope) ([]database.NotificationRoute, error) {
	if p == nil || len(p.routes) == 0 {
		return nil, nil
	}

	routes := make([]database.NotificationRoute, 0, len(p.routes))
	for _, route := range p.routes {
		if route.MessageType == "" {
			route.MessageType = event.EventName
		}
		routes = append(routes, route)
	}

	return routes, nil
}
