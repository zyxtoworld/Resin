package observability

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

func TestRedactAccountDoesNotContainInput(t *testing.T) {
	const input = "Bearer token username=user password=pass https://user:pass@example.invalid/sub?token=query X-API-Key=key Cookie=session"
	projector := NewProjector([]byte("deterministic-observability-key-000"))
	got := projector.RedactAccount("platform", input)
	if got == "" || got == input || len(got) != len("redacted-account-")+base64.RawURLEncoding.EncodedLen(sha256.Size) {
		t.Fatalf("account display did not produce an opaque value")
	}
	if contains(got, input) {
		t.Fatal("account display contains the full input")
	}
}

func TestLeaseIDRoundTrip(t *testing.T) {
	const platformID = "platform"
	const account = "Bearer secret"
	projector := NewProjector([]byte("deterministic-observability-key-000"))
	id := projector.LeaseID(platformID, account)
	if !projector.MatchesLeaseID(platformID, account, id) {
		t.Fatal("generated lease ID did not match its owner")
	}
	if projector.MatchesLeaseID(platformID, "other", id) || projector.MatchesLeaseID("other", account, id) {
		t.Fatal("lease ID matched a different owner")
	}
}

func TestLeaseIDFailsClosedAfterKeyRotation(t *testing.T) {
	const platformID = "platform"
	const account = "account"
	oldProjector := NewProjector([]byte("old-generation-key-for-tests-000000"))
	newProjector := NewProjector([]byte("new-generation-key-for-tests-000000"))
	oldID := oldProjector.LeaseID(platformID, account)
	newID := newProjector.LeaseID(platformID, account)
	if oldID == newID || len(oldID) != len(newID) {
		t.Fatal("projection-key rotation did not change the opaque lease ID")
	}
}

func TestNewProjectorRejectsWeakKey(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewProjector accepted a key shorter than sha256.Size")
		}
	}()
	NewProjector(make([]byte, sha256.Size-1))
}

func TestProjectorCopiesKey(t *testing.T) {
	key := make([]byte, sha256.Size)
	for i := range key {
		key[i] = byte(i + 1)
	}
	projector := NewProjector(key)
	before := projector.LeaseID("platform", "account")
	key[0] ^= 0xff
	if after := projector.LeaseID("platform", "account"); after != before {
		t.Fatal("Projector output changed after the caller mutated its key")
	}
}

func TestHMACDomainsAreSeparated(t *testing.T) {
	projector := NewProjector([]byte("deterministic-observability-key-000"))
	accountDigest := projector.digest("account-display/v1", "platform", "account")
	leaseDigest := projector.digest("lease-id/v1", "platform", "account")
	if accountDigest == leaseDigest {
		t.Fatal("account display and lease ID domains were not separated")
	}
}

func TestAccountDisplayIsPlatformBound(t *testing.T) {
	projector := NewProjector([]byte("deterministic-observability-key-000"))
	const account = "same-account"
	if got, want := projector.RedactAccount("platform-a", account), projector.RedactAccount("platform-a", account); got != want {
		t.Fatal("same platform/account projection was not deterministic")
	}
	if projector.RedactAccount("platform-a", account) == projector.RedactAccount("platform-b", account) {
		t.Fatal("cross-platform account projections were linkable")
	}
}

func TestProjectorTupleEncodingDoesNotAmbiguateFields(t *testing.T) {
	projector := NewProjector([]byte("deterministic-observability-key-000"))
	left := projector.LeaseID("platform-a\x00b", "account-c")
	right := projector.LeaseID("platform-a", "b\x00account-c")
	if left == right {
		t.Fatal("length-prefixed projector tuple encoding collided")
	}
}

func contains(value, needle string) bool {
	for i := 0; i+len(needle) <= len(value); i++ {
		if value[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
