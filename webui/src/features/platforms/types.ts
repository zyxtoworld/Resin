export type PlatformMissAction = "TREAT_AS_EMPTY" | "REJECT";
export type PlatformEmptyAccountBehavior = "RANDOM" | "FIXED_HEADER" | "ACCOUNT_HEADER_RULE";
export type PlatformAllocationPolicy = "BALANCED" | "PREFER_LOW_LATENCY" | "PREFER_IDLE_IP";
export type PlatformResponseFailureKind =
  | "timeout"
  | "transport_timeout"
  | "connect_timeout"
  | "response_header_timeout"
  | "first_byte_timeout"
  | "idle_timeout"
  | "transport_error";

export type PlatformResponseRule = {
  id: string;
  enabled: boolean;
  match: {
    status_codes?: number[];
    status_range?: Array<{ min: number; max: number }>;
    failure_kinds?: PlatformResponseFailureKind[];
    headers?: Array<{
      name: string;
      op: "exists" | "absent" | "regex" | "not_regex" | "contains" | "not_contains";
      value?: string;
    }>;
    body?: {
      op: "regex" | "not_regex" | "contains" | "not_contains";
      value: string;
    };
  };
  action: {
    type: "passthrough" | "retry_next" | "cooldown" | "cooldown_then_retry_next";
    cooldown_scope?: "egress_ip" | "route_entry";
    expiry_sources?: Array<{
      type: "retry_after" | "header" | "json_pointer" | "body_regex";
      header?: string;
      json_pointer?: string;
      regex?: string;
      capture?: number;
      format?: "rfc3339_utc" | "unix_seconds" | "unix_millis" | "delta_seconds";
    }>;
    fallback?: "next_utc_midnight" | "fixed_duration" | "none";
    fixed_duration?: string;
  };
};

export type Platform = {
  id: string;
  name: string;
  sticky_ttl: string;
  regex_filters: string[];
  region_filters: string[];
  response_rules: PlatformResponseRule[];
  routable_node_count: number;
  reverse_proxy_miss_action: PlatformMissAction;
  reverse_proxy_empty_account_behavior: PlatformEmptyAccountBehavior;
  reverse_proxy_fixed_account_header: string;
  allocation_policy: PlatformAllocationPolicy;
  passive_circuit_breaker_disabled: boolean;
  updated_at: string;
};

export type PageResponse<T> = {
  items: T[];
  total: number;
  limit: number;
  offset: number;
};

export type PlatformCreateInput = {
  name: string;
  sticky_ttl?: string;
  regex_filters?: string[];
  region_filters?: string[];
  response_rules?: PlatformResponseRule[];
  reverse_proxy_miss_action?: PlatformMissAction;
  reverse_proxy_empty_account_behavior?: PlatformEmptyAccountBehavior;
  reverse_proxy_fixed_account_header?: string;
  allocation_policy?: PlatformAllocationPolicy;
  passive_circuit_breaker_disabled?: boolean;
};

export type PlatformUpdateInput = {
  name?: string;
  sticky_ttl?: string;
  regex_filters?: string[];
  region_filters?: string[];
  response_rules?: PlatformResponseRule[];
  reverse_proxy_miss_action?: PlatformMissAction;
  reverse_proxy_empty_account_behavior?: PlatformEmptyAccountBehavior;
  reverse_proxy_fixed_account_header?: string;
  allocation_policy?: PlatformAllocationPolicy;
  passive_circuit_breaker_disabled?: boolean;
};

export type PlatformLease = {
  platform_id: string;
  account: string;
  account_redacted: boolean;
  lease_id: string;
  node_hash: string;
  node_tag: string;
  egress_ip: string;
  expiry: string;
  last_accessed: string;
};

export type PlatformLeaseSortBy = "account" | "expiry" | "last_accessed";
export type SortOrder = "asc" | "desc";

export type PlatformRouteNode = NodeSummary & {
  status: "available" | "cooling" | "circuit_open" | "not_ready" | "disabled";
  lease_count: number;
};

export type NodeSummary = {
  node_hash: string;
  created_at: string;
  enabled: boolean;
  display_tag?: string;
  has_outbound: boolean;
  last_error?: string;
  circuit_open_since?: string | null;
  failure_count: number;
  egress_ip?: string;
  region?: string;
  last_egress_update?: string;
  last_latency_probe_attempt?: string;
  last_authority_latency_probe_attempt?: string;
  reference_latency_ms?: number | null;
  last_egress_update_attempt?: string;
  tags: Array<{
    subscription_id: string;
    subscription_name: string;
    tag: string;
  }>;
};

export type PlatformCooldownSnapshot = {
  scope: "egress_ip" | "route_entry";
  node_hash?: string;
  egress_ip?: string;
  until: string;
};

export type PlatformRouteState = {
  platform_id: string;
  observed_at: string;
  nodes: PlatformRouteNode[];
  nodes_total: number;
  nodes_limit: number;
  nodes_has_more: boolean;
  nodes_next_cursor?: string;
  leases: {
    items: PlatformLease[];
    total: number;
    limit: number;
    has_more: boolean;
    next_cursor?: string;
  };
  cooldowns: PlatformCooldownSnapshot[];
  cooldowns_total: number;
};

export type ListPlatformLeasesInput = {
  limit?: number;
  offset?: number;
  account?: string;
  fuzzy?: boolean;
  sort_by?: PlatformLeaseSortBy;
  sort_order?: SortOrder;
};
