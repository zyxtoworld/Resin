package metrics

import (
	"path/filepath"
	"testing"
)

func TestMetricsRepo_QueryAccessLatencyFailsClosedOnMalformedBucketsJSON(t *testing.T) {
	repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewMetricsRepo: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	if _, err := repo.db.Exec(`INSERT INTO metric_access_latency_bucket
		(bucket_start_unix, platform_id, buckets_json) VALUES
		(1, NULL, '[1,2,3]'),
		(2, NULL, '{"not":"a histogram"}')`); err != nil {
		t.Fatalf("insert access latency fixtures: %v", err)
	}

	rows, err := repo.QueryAccessLatency(1, 2, "")
	if err == nil {
		t.Fatalf("QueryAccessLatency returned %d rows without reporting malformed buckets_json", len(rows))
	}
	if rows != nil {
		t.Fatalf("QueryAccessLatency returned partial rows on malformed buckets_json: %+v", rows)
	}
}
