package metrics

import (
	"path/filepath"
	"testing"
)

func TestMetricsRepo_QueryAccessLatencyFailsClosedOnMalformedBucketsJSON(t *testing.T) {
	tests := []struct {
		name      string
		buckets   string
		wantError bool
	}{
		{name: "empty array", buckets: "[]"},
		{name: "normal array", buckets: "[1,2,3]"},
		{name: "object shape", buckets: `{"not":"a histogram"}`, wantError: true},
		{name: "null", buckets: "null", wantError: true},
		{name: "negative count", buckets: "[1,-1]", wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
			if err != nil {
				t.Fatalf("NewMetricsRepo: %v", err)
			}
			t.Cleanup(func() { _ = repo.Close() })

			if _, err := repo.db.Exec(`INSERT INTO metric_access_latency_bucket
				(bucket_start_unix, platform_id, buckets_json) VALUES (?, NULL, ?)`, 1, tc.buckets); err != nil {
				t.Fatalf("insert access latency fixture: %v", err)
			}

			rows, err := repo.QueryAccessLatency(1, 1, "")
			if tc.wantError {
				if err == nil {
					t.Fatalf("QueryAccessLatency returned %d rows without rejecting buckets_json=%q", len(rows), tc.buckets)
				}
				if rows != nil {
					t.Fatalf("QueryAccessLatency returned partial rows for buckets_json=%q: %+v", tc.buckets, rows)
				}
				return
			}
			if err != nil {
				t.Fatalf("QueryAccessLatency(%q): %v", tc.buckets, err)
			}
			if len(rows) != 1 {
				t.Fatalf("QueryAccessLatency(%q) returned %d rows, want 1", tc.buckets, len(rows))
			}
		})
	}
}
