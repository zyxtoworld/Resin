package routing

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/Resinat/Resin/internal/node"
)

const nodePageCursorDomain = "resin-route-state-node-cursor-v1\x00"

var ErrNodePageCursorInvalid = errors.New("invalid node route-state cursor")

type nodePageCursor struct {
	Version    int    `json:"version"`
	PlatformID string `json:"platform_id"`
	Generation uint64 `json:"generation"`
	Status     string `json:"status"`
	Limit      int    `json:"limit"`
	LastHash   string `json:"last_hash"`
	MAC        string `json:"mac"`
}

func nodePageCursorMAC(payload []byte) []byte {
	hasher := hmac.New(sha256.New, leaseCursorSecret)
	_, _ = hasher.Write([]byte(nodePageCursorDomain))
	_, _ = hasher.Write(payload)
	return hasher.Sum(nil)
}

// EncodeNodePageCursor creates a process-local, platform/generation-bound
// keyset cursor. The cursor contains only the last stable node hash; the
// signing key is deliberately shared with lease cursors but domain-separated.
func EncodeNodePageCursor(platformID string, generation uint64, status string, limit int, last node.Hash) string {
	if strings.TrimSpace(platformID) == "" || limit <= 0 || last == node.Zero {
		return ""
	}
	base := nodePageCursor{
		Version:    1,
		PlatformID: platformID,
		Generation: generation,
		Status:     status,
		Limit:      limit,
		LastHash:   last.Hex(),
	}
	payload, err := json.Marshal(base)
	if err != nil {
		return ""
	}
	base.MAC = base64.RawURLEncoding.EncodeToString(nodePageCursorMAC(payload))
	raw, err := json.Marshal(base)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// DecodeNodePageCursor validates the cursor contract before any route-state
// scan. The caller must compare the returned generation with the current
// routing state while holding its read owner.
func DecodeNodePageCursor(raw, platformID, status string, limit int) (node.Hash, uint64, error) {
	if strings.TrimSpace(raw) == "" || len(raw) > 2048 || strings.TrimSpace(platformID) == "" || limit <= 0 {
		return node.Zero, 0, ErrNodePageCursorInvalid
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return node.Zero, 0, ErrNodePageCursorInvalid
	}
	var cursor nodePageCursor
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return node.Zero, 0, ErrNodePageCursorInvalid
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return node.Zero, 0, ErrNodePageCursorInvalid
	}
	if cursor.Version != 1 || cursor.PlatformID != platformID || cursor.Status != status || cursor.Limit != limit || cursor.LastHash == "" || cursor.MAC == "" {
		return node.Zero, 0, ErrNodePageCursorInvalid
	}
	last, err := node.ParseHex(cursor.LastHash)
	if err != nil || last == node.Zero {
		return node.Zero, 0, ErrNodePageCursorInvalid
	}
	providedMAC, err := base64.RawURLEncoding.DecodeString(cursor.MAC)
	if err != nil || !hmac.Equal(providedMAC, nodePageCursorMAC(mustMarshalNodeCursorWithoutMAC(cursor))) {
		return node.Zero, 0, ErrNodePageCursorInvalid
	}
	return last, cursor.Generation, nil
}

func mustMarshalNodeCursorWithoutMAC(cursor nodePageCursor) []byte {
	cursor.MAC = ""
	payload, err := json.Marshal(cursor)
	if err != nil {
		return nil
	}
	return payload
}
