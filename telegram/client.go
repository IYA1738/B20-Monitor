package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	botToken   string
	httpClient *http.Client
}

func NewClient(botToken string, timeout time.Duration) (*Client, error) {
	if botToken == "" {
		return nil, fmt.Errorf("telegram bot token is required")
	}

	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	return &Client{
		botToken: botToken,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}, nil
}

type sendMessageRequest struct {
	ChatID               string `json:"chat_id"`
	Text                 string `json:"text"`
	ParseMode            string `json:"parse_mode,omitempty"`
	DisableWebPageReview bool   `json:"disable_web_page_preview,omitempty"`
}

type telegramResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
	ErrorCode   int    `json:"error_code"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

type RetryAfterError struct {
	StatusCode  int
	ErrorCode   int
	Description string
	RetryAfter  time.Duration
	Body        string
}

func (e *RetryAfterError) Error() string {
	return fmt.Sprintf(
		"telegram sendMessage failed status=%d error_code=%d description=%s retry_after=%s body=%s",
		e.StatusCode,
		e.ErrorCode,
		e.Description,
		e.RetryAfter,
		e.Body,
	)
}

func (e *RetryAfterError) RetryAfterDelay() time.Duration {
	if e == nil {
		return 0
	}

	return e.RetryAfter
}

func (c *Client) SendMessage(ctx context.Context, chatID string, text string) error {
	if c == nil || c.httpClient == nil {
		return fmt.Errorf("telegram client is nil")
	}

	if chatID == "" {
		return fmt.Errorf("telegram chat id is required")
	}

	if text == "" {
		return fmt.Errorf("telegram message text is required")
	}

	body := sendMessageRequest{
		ChatID:               chatID,
		Text:                 text,
		ParseMode:            "HTML",
		DisableWebPageReview: false, // 暂时先不展开tg的网页预览
	}

	rawBody, err := json.Marshal(body)

	if err != nil {
		return fmt.Errorf("marshal telegram sendMessage request: %w", err)
	}

	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.botToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(rawBody))
	if err != nil {
		return fmt.Errorf("new telegram sendMessage request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do telegram sendMessage request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)

	if err != nil {
		return fmt.Errorf("read telegram sendMessage response: %w", err)
	}

	var tgResp telegramResponse
	if err := json.Unmarshal(respBody, &tgResp); err != nil {
		return fmt.Errorf("decode telegram sendMessage response: %w body=%s", err, string(respBody))
	}

	if !tgResp.OK {
		if tgResp.Parameters.RetryAfter > 0 {
			return &RetryAfterError{
				StatusCode:  resp.StatusCode,
				ErrorCode:   tgResp.ErrorCode,
				Description: tgResp.Description,
				RetryAfter:  time.Duration(tgResp.Parameters.RetryAfter) * time.Second,
				Body:        string(respBody),
			}
		}

		return fmt.Errorf(
			"telegram sendMessage failed status=%d error_code=%d description=%s body=%s",
			resp.StatusCode,
			tgResp.ErrorCode,
			tgResp.Description,
			string(respBody),
		)
	}

	return nil

}
