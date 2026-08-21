package routing

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/Resinat/Resin/internal/node"
)

func TestNodePageCursorRejectsTamperingTrailingDataAndContractChanges(t *testing.T) {
	hash := node.HashFromRawOptions([]byte(`{"id":"node-page-cursor"}`))
	cursor := EncodeNodePageCursor("platform-a", 7, "available", 50, hash)
	if cursor == "" {
		t.Fatal("empty node page cursor")
	}
	gotHash, gotGeneration, err := DecodeNodePageCursor(cursor, "platform-a", "available", 50)
	if err != nil || gotHash != hash || gotGeneration != 7 {
		t.Fatalf("valid cursor = %s/%d/%v, want %s/7/nil", gotHash.Hex(), gotGeneration, err, hash.Hex())
	}

	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(decoded, &payload); err != nil {
		t.Fatalf("decode cursor JSON: %v", err)
	}
	payload["last_hash"] = node.HashFromRawOptions([]byte(`{"id":"tampered"}`)).Hex()
	tamperedJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal tampered cursor: %v", err)
	}
	tampered := base64.RawURLEncoding.EncodeToString(tamperedJSON)
	if _, _, err := DecodeNodePageCursor(tampered, "platform-a", "available", 50); err == nil {
		t.Fatal("tampered cursor was accepted")
	}

	trailing := base64.RawURLEncoding.EncodeToString(append(decoded, []byte(` {}`)...))
	if _, _, err := DecodeNodePageCursor(trailing, "platform-a", "available", 50); err == nil {
		t.Fatal("cursor with trailing JSON was accepted")
	}
	for name, args := range map[string][3]string{
		"platform": {"platform-b", "available", "50"},
		"status":   {"platform-a", "cooling", "50"},
		"limit":    {"platform-a", "available", "25"},
	} {
		if _, _, err := DecodeNodePageCursor(cursor, args[0], args[1], atoiForCursorTest(t, args[2])); err == nil {
			t.Fatalf("cursor accepted changed %s contract", name)
		}
	}
}

func atoiForCursorTest(t *testing.T, value string) int {
	t.Helper()
	if value == "50" {
		return 50
	}
	return 25
}
