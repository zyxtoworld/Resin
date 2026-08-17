export type PlatformMissAction = "TREAT_AS_EMPTY" | "REJECT";
export type PlatformEmptyAccountBehavior = "RANDOM" | "FIXED_HEADER" | "ACCOUNT_HEADER_RULE";
export type PlatformAllocationPolicy = "BALANCED" | "PREFER_LOW_LATENCY" | "PREFER_IDLE_IP";

export type PlatformResponseRule = {
  id: string;
  enabled: boolean;
  match: {
    status_codes?: number[];
    status_range?: Array<{ min: number; max: number }>;
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
  node_hash: string;
  node_tag: string;
  egress_ip: string;
  expiry: string;
  last_accessed: string;
};

export type PlatformLeaseSortBy = "account" | "expiry" | "last_accessed";
export type SortOrder = "asc" | "desc";

export type ListPlatformLeasesInput = {
  limit?: number;
  offset?: number;
  account?: string;
  fuzzy?: boolean;
  sort_by?: PlatformLeaseSortBy;
  sort_order?: SortOrder;
};
