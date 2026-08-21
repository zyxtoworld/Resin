ALTER TABLE platforms
    ADD COLUMN proxy_request_attempt_timeout_ns INTEGER NOT NULL DEFAULT 0;

ALTER TABLE platforms
    ADD COLUMN proxy_request_max_attempts INTEGER NOT NULL DEFAULT 0;
