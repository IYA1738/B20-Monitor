package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"time"

	"token-discover-demo/database"
	"token-discover-demo/telegram"

	"golang.org/x/time/rate"
)

type TelegramSender struct {
	client  *telegram.Client
	limiter *rate.Limiter
}

func NewTelegramSender(client *telegram.Client) (*TelegramSender, error) {
	if client == nil {
		return nil, fmt.Errorf("telegram client is nil")
	}

	return &TelegramSender{
		client:  client,
		limiter: rate.NewLimiter(rate.Every(3*time.Second), 1),
	}, nil
}

func (s *TelegramSender) Channel() string {
	return database.NotificationChannelTelegram
}

func (s *TelegramSender) Send(ctx context.Context, task database.NotificationOutbox) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("telegram sender is nil")
	}

	if s.limiter != nil {
		if err := s.limiter.Wait(ctx); err != nil {
			return fmt.Errorf("wait telegram rate limiter: %w", err)
		}
	}

	payload, err := database.DecodeNotificationPayload(task.Payload)
	if err != nil {
		return err
	}

	text := formatTelegramNotification(task, payload)
	return s.client.SendMessage(ctx, task.Target, text)
}

func formatTelegramNotification(task database.NotificationOutbox, payload database.NotificationPayload) string {
	event := payload.Event
	fields := map[string]any{}
	_ = json.Unmarshal(event.Payload, &fields)

	if strings.EqualFold(task.MessageType, "b20_created") || strings.EqualFold(event.EventName, "b20_created") {
		return formatB20CreatedTelegram(event, fields)
	}

	return formatGenericTelegram(event)
}

func formatB20CreatedTelegram(event database.NotificationEventPayload, fields map[string]any) string {
	name := getString(fields, "name", "Name")
	symbol := getString(fields, "symbol", "Symbol")
	decimals := getString(fields, "decimals", "Decimals")
	token := getString(fields, "token", "Token", "token_address", "TokenAddress")
	// variant := getString(fields, "variant", "Variant")

	if token == "" {
		token = event.ContractAddress
	}

	txURL := explorerTxURL(event.Network, event.TxHash)

	var b strings.Builder
	b.WriteString("<b>B20 Created</b>\n\n")

	if name != "" {
		b.WriteString("<b>代币名称:</b> ")
		b.WriteString(html.EscapeString(name))
		b.WriteString("\n")
	}

	if symbol != "" {
		b.WriteString("<b>代币符号:</b> ")
		b.WriteString(html.EscapeString(symbol))
		b.WriteString("\n")
	}

	if decimals != "" {
		b.WriteString("<b>小数位:</b> ")
		b.WriteString(html.EscapeString(decimals))
		b.WriteString("\n")
	}

	// if variant != "" {
	// 	b.WriteString("<b>Variant:</b> ")
	// 	b.WriteString(html.EscapeString(variant))
	// 	b.WriteString("\n")
	// }

	b.WriteString("<b>合约地址:</b> <code>")
	b.WriteString(html.EscapeString(token))
	b.WriteString("</code>\n")

	writeTelegramEventTail(&b, event, txURL)

	return b.String()
}

func formatGenericTelegram(event database.NotificationEventPayload) string {
	txURL := explorerTxURL(event.Network, event.TxHash)

	var b strings.Builder
	b.WriteString("<b>")
	b.WriteString(html.EscapeString(event.EventName))
	b.WriteString("</b>\n\n")

	b.WriteString("<b>合约地址:</b> <code>")
	b.WriteString(html.EscapeString(event.ContractAddress))
	b.WriteString("</code>\n")

	writeTelegramEventTail(&b, event, txURL)

	return b.String()
}

func writeTelegramEventTail(b *strings.Builder, event database.NotificationEventPayload, txURL string) {
	b.WriteString("<b>链:</b> ")
	b.WriteString(html.EscapeString(event.ChainName))
	b.WriteString(" / ")
	b.WriteString(html.EscapeString(event.Network))
	b.WriteString("\n")

	b.WriteString("<b>当前区块:</b> ")
	b.WriteString(fmt.Sprintf("%d", event.BlockNumber))
	b.WriteString("\n")

	b.WriteString("<b>交易哈希:</b> ")
	if txURL != "" {
		b.WriteString("<a href=\"")
		b.WriteString(html.EscapeString(txURL))
		b.WriteString("\">")
		b.WriteString(html.EscapeString(shortHash(event.TxHash)))
		b.WriteString("</a>")
	} else {
		b.WriteString("<code>")
		b.WriteString(html.EscapeString(event.TxHash))
		b.WriteString("</code>")
	}
}

func getString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok || value == nil {
			continue
		}

		switch v := value.(type) {
		case string:
			return v
		case float64:
			if v == float64(uint64(v)) {
				return fmt.Sprintf("%d", uint64(v))
			}
			return fmt.Sprintf("%v", v)
		default:
			return fmt.Sprintf("%v", v)
		}
	}

	return ""
}

func shortHash(v string) string {
	if len(v) <= 14 {
		return v
	}

	return v[:10] + "..." + v[len(v)-4:]
}

func explorerTxURL(network string, tx string) string {
	switch network {
	case "base-sepolia":
		return "https://sepolia.basescan.org/tx/" + tx
	case "base":
		return "https://basescan.org/tx/" + tx
	default:
		return ""
	}
}
