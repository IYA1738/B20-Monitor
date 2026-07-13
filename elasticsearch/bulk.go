package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	BulkOperationIndex  = "index"
	BulkOperationUpdate = "update"
	BulkOperationDelete = "delete"
)

type BulkDocument struct {
	IndexName  string
	DocumentID string
	Operation  string

	Payload json.RawMessage

	Routing         string
	RetryOnConflict int
	DocAsUpsert     bool
}

type BulkResult struct {
	Took      time.Duration
	Total     int
	Succeeded int
	Failed    int
	Items     []BulkItemResult
}

type BulkItemResult struct {
	Operation   string
	IndexName   string
	DocumentID  string
	Status      int
	Result      string
	ErrorType   string
	ErrorReason string
}

func (i BulkItemResult) Ok() bool {
	return i.Status >= 200 && i.Status < 300 && i.ErrorType == ""
}

type BulkPartialError struct {
	Failed int
	Total  int
}

func (e *BulkPartialError) Error() string {
	return fmt.Sprintf("elasticsearch bulk partial failure: failed=%d total=%d", e.Failed, e.Total)
}

func (c *Client) Bulk(ctx context.Context, docs []BulkDocument) (*BulkResult, error) {
	if c == nil || c.raw == nil {
		return nil, fmt.Errorf("ES client is nil")
	}

	if len(docs) == 0 {
		return &BulkResult{}, nil
	}

	var body bytes.Buffer
	enc := json.NewEncoder(&body)

	for i := range docs {
		if err := validateBulkDocument(docs[i]); err != nil {
			return nil, fmt.Errorf("invalid bulk document index=%d: %w", i, err)
		}

		if err := writeBulkDocument(enc, docs[i]); err != nil {
			return nil, fmt.Errorf("write bulk document index=%d: %w", i, err)
		}
	}

	resp, err := c.raw.Bulk(
		bytes.NewReader(body.Bytes()),
		c.raw.Bulk.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("execute ES bulk: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read ES bulk response: %w", err)
	}

	if resp.IsError() {
		return nil, fmt.Errorf("ES bulk http error status=%s body=%s", resp.Status(), string(raw))
	}

	result, err := parseBulkResponse(raw)
	if err != nil {
		return nil, err
	}

	if result.Failed > 0 {
		return result, &BulkPartialError{
			Failed: result.Failed,
			Total:  result.Total,
		}
	}

	return result, nil
}

func normalizeBulkOperation(operation string) string {
	operation = strings.ToLower(strings.TrimSpace(operation))
	if operation == "" {
		return BulkOperationIndex
	}
	return operation
}

func validateBulkDocument(doc BulkDocument) error {
	if doc.IndexName == "" {
		return fmt.Errorf("index name is required")
	}

	if doc.DocumentID == "" {
		return fmt.Errorf("document id is required")
	}

	switch normalizeBulkOperation(doc.Operation) {
	case BulkOperationIndex, BulkOperationUpdate:
		if len(doc.Payload) == 0 {
			return fmt.Errorf("payload is required for operation=%s", normalizeBulkOperation(doc.Operation))
		}

		if !json.Valid(doc.Payload) {
			return fmt.Errorf("payload is not valid json")
		}

	case BulkOperationDelete:
	default:
		return fmt.Errorf("unsupported bulk operation: %s", doc.Operation)
	}

	return nil
}

func writeBulkDocument(enc *json.Encoder, doc BulkDocument) error {
	operation := normalizeBulkOperation(doc.Operation)

	meta := map[string]any{
		"_index": doc.IndexName,
		"_id":    doc.DocumentID,
	}

	if doc.Routing != "" {
		meta["routing"] = doc.Routing
	}

	if operation == BulkOperationUpdate && doc.RetryOnConflict > 0 {
		meta["retry_on_conflict"] = doc.RetryOnConflict
	}

	action := map[string]any{
		operation: meta,
	}

	if err := enc.Encode(action); err != nil {
		return err
	}

	switch operation {
	case BulkOperationIndex:
		return enc.Encode(doc.Payload)

	case BulkOperationUpdate:
		updateBody := map[string]any{
			"doc": doc.Payload,
		}

		if doc.DocAsUpsert {
			updateBody["doc_as_upsert"] = true
		}

		return enc.Encode(updateBody)

	case BulkOperationDelete:
		return nil

	default:
		return fmt.Errorf("unsupported bulk operation: %s", doc.Operation)
	}
}

type bulkResponse struct {
	Took   int64                         `json:"took"`
	Errors bool                          `json:"errors"`
	Items  []map[string]bulkItemResponse `json:"items"`
}

type bulkItemResponse struct {
	IndexName  string `json:"_index"`
	DocumentID string `json:"_id"`

	Status int    `json:"status"`
	Result string `json:"result"`

	Error *bulkItemError `json:"error"`
}

type bulkItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`

	CausedBy *bulkItemError `json:"caused_by"`
}

func parseBulkResponse(raw []byte) (*BulkResult, error) {
	var resp bulkResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("decode ES bulk response: %w body=%s", err, string(raw))
	}

	result := &BulkResult{
		Took:  time.Duration(resp.Took) * time.Millisecond,
		Total: len(resp.Items),
		Items: make([]BulkItemResult, 0, len(resp.Items)),
	}

	for _, item := range resp.Items {
		for operation, detail := range item {
			parsed := BulkItemResult{
				Operation:  operation,
				IndexName:  detail.IndexName,
				DocumentID: detail.DocumentID,
				Status:     detail.Status,
				Result:     detail.Result,
			}

			if detail.Error != nil {
				parsed.ErrorType = detail.Error.Type
				parsed.ErrorReason = detail.Error.Message()
			}

			if parsed.Ok() {
				result.Succeeded++
			} else {
				result.Failed++
			}

			result.Items = append(result.Items, parsed)
		}
	}

	return result, nil
}

func (e *bulkItemError) Message() string {
	if e == nil {
		return ""
	}

	var parts []string

	if e.Type != "" {
		parts = append(parts, e.Type)
	}

	if e.Reason != "" {
		parts = append(parts, e.Reason)
	}

	if e.CausedBy != nil {
		causedBy := e.CausedBy.Message()
		if causedBy != "" {
			parts = append(parts, "caused_by: "+causedBy)
		}
	}

	return strings.Join(parts, ": ")
}
