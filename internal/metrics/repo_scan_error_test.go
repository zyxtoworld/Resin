package metrics

import (
	"path/filepath"
	"testing"
)

func TestMetricsRepo_QueriesFailClosedOnScanError(t *testing.T) {
	tests := []struct {
		name     string
		setupSQL string
		query    func(*MetricsRepo) (int, error)
	}{
		{
			name: "traffic",
			setupSQL: `INSERT INTO metric_traffic_bucket
				(bucket_start_unix, ingress_bytes, egress_bytes) VALUES
				(1, 10, 20),
				(2, 'not-an-integer', 30)`,
			query: func(repo *MetricsRepo) (int, error) {
				rows, err := repo.QueryTraffic(1, 2)
				return len(rows), err
			},
		},
		{
			name: "requests",
			setupSQL: `INSERT INTO metric_request_bucket
				(bucket_start_unix, platform_id, total_requests, success_requests) VALUES
				(1, 'plat', 10, 9),
				(2, 'plat', 'not-an-integer', 9)`,
			query: func(repo *MetricsRepo) (int, error) {
				rows, err := repo.QueryRequests(1, 2, "plat")
				return len(rows), err
			},
		},
		{
			name: "probes",
			setupSQL: `INSERT INTO metric_probe_bucket
				(bucket_start_unix, total_count) VALUES
				(1, 10),
				(2, 'not-an-integer')`,
			query: func(repo *MetricsRepo) (int, error) {
				rows, err := repo.QueryProbes(1, 2)
				return len(rows), err
			},
		},
		{
			name: "node_pool",
			setupSQL: `INSERT INTO metric_node_pool_bucket
				(bucket_start_unix, total_nodes, healthy_nodes, egress_ip_count) VALUES
				(1, 10, 9, 8),
				(2, 'not-an-integer', 9, 8)`,
			query: func(repo *MetricsRepo) (int, error) {
				rows, err := repo.QueryNodePool(1, 2)
				return len(rows), err
			},
		},
		{
			name: "access_latency",
			setupSQL: `INSERT INTO metric_access_latency_bucket
				(bucket_start_unix, platform_id, buckets_json) VALUES
				(1, 'plat', '[]'),
				(1.5, 'plat', '[]')`,
			query: func(repo *MetricsRepo) (int, error) {
				rows, err := repo.QueryAccessLatency(1, 2, "plat")
				return len(rows), err
			},
		},
		{
			name: "lease_lifetime",
			setupSQL: `INSERT INTO metric_lease_lifetime_bucket
				(bucket_start_unix, platform_id, sample_count, p1_ms, p5_ms, p50_ms) VALUES
				(1, 'plat', 10, 1, 5, 50),
				(2, 'plat', 'not-an-integer', 1, 5, 50)`,
			query: func(repo *MetricsRepo) (int, error) {
				rows, err := repo.QueryLeaseLifetime(1, 2, "plat")
				return len(rows), err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo, err := NewMetricsRepo(filepath.Join(t.TempDir(), "metrics.db"))
			if err != nil {
				t.Fatalf("NewMetricsRepo: %v", err)
			}
			t.Cleanup(func() { _ = repo.Close() })

			if _, err := repo.db.Exec(test.setupSQL); err != nil {
				t.Fatalf("insert fixtures: %v", err)
			}

			rowCount, err := test.query(repo)
			if err == nil {
				t.Fatalf("query returned %d partial rows without reporting the malformed persisted row", rowCount)
			}
			if rowCount != 0 {
				t.Fatalf("query returned %d rows, want none on scan failure", rowCount)
			}
		})
	}
}
