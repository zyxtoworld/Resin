package observability

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
)

// Projector is immutable after construction. Its key is process-generation
// scoped; a restarted process intentionally cannot resolve old opaque IDs.
type Projector struct {
	key []byte
}

func NewProjector(key []byte) *Projector {
	if len(key) < sha256.Size {
		panic("observability: projection key is shorter than sha256.Size")
	}
	return &Projector{key: append([]byte(nil), key...)}
}

func NewRandomProjector() *Projector {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic("observability: cannot initialize account projection key")
	}
	return NewProjector(key)
}

// RedactAccount returns a full-length, platform-bound non-reversible account
// identifier for display. The raw account remains an internal routing key.
func (p *Projector) RedactAccount(platformID, account string) string {
	if account == "" {
		return ""
	}
	digest := p.digest("account-display/v1", platformID, account)
	return "redacted-account-" + base64.RawURLEncoding.EncodeToString(digest[:])
}

// LeaseID returns an opaque operation token for a platform/account pair.
func (p *Projector) LeaseID(platformID, account string) string {
	digest := p.digest("lease-id/v1", platformID, account)
	return "lease-" + base64.RawURLEncoding.EncodeToString(digest[:])
}

func (p *Projector) MatchesLeaseID(platformID, account, leaseID string) bool {
	return IsLeaseID(leaseID) && hmac.Equal([]byte(p.LeaseID(platformID, account)), []byte(leaseID))
}

func (p *Projector) digest(domain string, parts ...string) [sha256.Size]byte {
	h := hmac.New(sha256.New, p.key)
	writeDigestPart(h, domain)
	for _, part := range parts {
		writeDigestPart(h, part)
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func writeDigestPart(h interface{ Write([]byte) (int, error) }, part string) {
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(part)))
	_, _ = h.Write(length[:n])
	_, _ = h.Write([]byte(part))
}

func IsLeaseID(value string) bool {
	const prefix = "lease-"
	if len(value) != len(prefix)+base64.RawURLEncoding.EncodedLen(sha256.Size) || len(value) < len(prefix) {
		return false
	}
	if value[:len(prefix)] != prefix {
		return false
	}
	_, err := base64.RawURLEncoding.DecodeString(value[len(prefix):])
	return err == nil
}
