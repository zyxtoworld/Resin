ALTER TABLE platforms
ADD COLUMN response_rules_json TEXT NOT NULL DEFAULT '[]';
