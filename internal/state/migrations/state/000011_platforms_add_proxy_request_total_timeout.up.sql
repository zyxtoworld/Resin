ALTER TABLE platforms
    ADD COLUMN proxy_request_total_timeout_ns INTEGER NOT NULL DEFAULT 0;
