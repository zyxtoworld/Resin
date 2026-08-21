import { apiRequest } from "../../lib/api-client";
import type {
  ListPlatformLeasesInput,
  PageResponse,
  Platform,
  PlatformResponseRule,
  PlatformCreateInput,
  PlatformLease,
  PlatformRouteState,
  PlatformUpdateInput,
} from "./types";

const basePath = "/api/v1/platforms";

type ApiPlatform = Omit<Platform, "regex_filters" | "region_filters"> & {
  regex_filters?: string[] | null;
  region_filters?: string[] | null;
  response_rules?: PlatformResponseRule[] | null;
  routable_node_count?: number | null;
  reverse_proxy_miss_action?: Platform["reverse_proxy_miss_action"] | null;
  reverse_proxy_empty_account_behavior?: Platform["reverse_proxy_empty_account_behavior"] | null;
  reverse_proxy_fixed_account_header?: string | null;
  passive_circuit_breaker_disabled?: boolean | null;
};

type ApiPlatformLease = Partial<PlatformLease>;

function parseMissAction(raw: ApiPlatform["reverse_proxy_miss_action"]): Platform["reverse_proxy_miss_action"] {
  if (raw === "TREAT_AS_EMPTY" || raw === "REJECT") {
    return raw;
  }
  throw new Error(`invalid reverse_proxy_miss_action: ${String(raw)}`);
}

function normalizePlatform(raw: ApiPlatform): Platform {
  return {
    ...raw,
    proxy_request_total_timeout:
      typeof raw.proxy_request_total_timeout === "string" ? raw.proxy_request_total_timeout : "",
    reverse_proxy_miss_action: parseMissAction(raw.reverse_proxy_miss_action),
    regex_filters: Array.isArray(raw.regex_filters) ? raw.regex_filters : [],
    region_filters: Array.isArray(raw.region_filters) ? raw.region_filters : [],
    response_rules: Array.isArray(raw.response_rules) ? raw.response_rules : [],
    routable_node_count: typeof raw.routable_node_count === "number" ? raw.routable_node_count : 0,
    reverse_proxy_empty_account_behavior:
      raw.reverse_proxy_empty_account_behavior === "RANDOM" ||
      raw.reverse_proxy_empty_account_behavior === "FIXED_HEADER" ||
      raw.reverse_proxy_empty_account_behavior === "ACCOUNT_HEADER_RULE"
        ? raw.reverse_proxy_empty_account_behavior
        : "RANDOM",
    reverse_proxy_fixed_account_header:
      typeof raw.reverse_proxy_fixed_account_header === "string" ? raw.reverse_proxy_fixed_account_header : "",
    passive_circuit_breaker_disabled:
      typeof raw.passive_circuit_breaker_disabled === "boolean" ? raw.passive_circuit_breaker_disabled : false,
  };
}

function normalizePlatformPage(raw: PageResponse<ApiPlatform>): PageResponse<Platform> {
  return {
    ...raw,
    items: raw.items.map(normalizePlatform),
  };
}

function normalizeLease(raw: ApiPlatformLease): PlatformLease {
  return {
    platform_id: typeof raw.platform_id === "string" ? raw.platform_id : "",
    account: typeof raw.account === "string" ? raw.account : "",
    account_redacted: raw.account_redacted === true,
    lease_id: typeof raw.lease_id === "string" ? raw.lease_id : "",
    node_hash: typeof raw.node_hash === "string" ? raw.node_hash : "",
    node_tag: typeof raw.node_tag === "string" ? raw.node_tag : "",
    egress_ip: typeof raw.egress_ip === "string" ? raw.egress_ip : "",
    expiry: typeof raw.expiry === "string" ? raw.expiry : "",
    last_accessed: typeof raw.last_accessed === "string" ? raw.last_accessed : "",
  };
}

function normalizeLeasePage(raw: PageResponse<ApiPlatformLease>): PageResponse<PlatformLease> {
  return {
    ...raw,
    items: raw.items.map(normalizeLease),
  };
}

export type ListPlatformsPageInput = {
  limit?: number;
  offset?: number;
  keyword?: string;
};

export async function listPlatforms(input: ListPlatformsPageInput = {}): Promise<PageResponse<Platform>> {
  const query = new URLSearchParams({
    limit: String(input.limit ?? 50),
    offset: String(input.offset ?? 0),
    sort_by: "name",
    sort_order: "asc",
  });
  const keyword = input.keyword?.trim();
  if (keyword) {
    query.set("keyword", keyword);
  }

  const data = await apiRequest<PageResponse<ApiPlatform>>(`${basePath}?${query.toString()}`);
  return normalizePlatformPage(data);
}

export async function getPlatform(id: string): Promise<Platform> {
  const data = await apiRequest<ApiPlatform>(`${basePath}/${id}`);
  return normalizePlatform(data);
}

export async function createPlatform(input: PlatformCreateInput): Promise<Platform> {
  const data = await apiRequest<ApiPlatform>(basePath, {
    method: "POST",
    body: input,
  });
  return normalizePlatform(data);
}

export async function updatePlatform(id: string, input: PlatformUpdateInput): Promise<Platform> {
  const data = await apiRequest<ApiPlatform>(`${basePath}/${id}`, {
    method: "PATCH",
    body: input,
  });
  return normalizePlatform(data);
}

export async function deletePlatform(id: string): Promise<void> {
  await apiRequest<void>(`${basePath}/${id}`, {
    method: "DELETE",
  });
}

export async function resetPlatform(id: string): Promise<Platform> {
  const data = await apiRequest<ApiPlatform>(`${basePath}/${id}/actions/reset-to-default`, {
    method: "POST",
  });
  return normalizePlatform(data);
}

export async function rebuildPlatform(id: string): Promise<void> {
  await apiRequest<{ status: "ok" }>(`${basePath}/${id}/actions/rebuild-routable-view`, {
    method: "POST",
  });
}

export async function listPlatformLeases(id: string, input: ListPlatformLeasesInput = {}): Promise<PageResponse<PlatformLease>> {
  const query = new URLSearchParams({
    limit: String(input.limit ?? 50),
    offset: String(input.offset ?? 0),
    sort_by: input.sort_by ?? "expiry",
    sort_order: input.sort_order ?? "asc",
  });

  const account = input.account?.trim();
  if (account) {
    query.set("account", account);
  }
  if (input.fuzzy !== undefined) {
    query.set("fuzzy", String(input.fuzzy));
  }

  const data = await apiRequest<PageResponse<ApiPlatformLease>>(`${basePath}/${id}/leases?${query.toString()}`);
  return normalizeLeasePage(data);
}

export async function deletePlatformLease(id: string, leaseID: string): Promise<void> {
	await apiRequest<void>(`${basePath}/${id}/leases/${encodeURIComponent(leaseID)}`, {
    method: "DELETE",
  });
}

export async function clearAllPlatformLeases(id: string): Promise<void> {
  await apiRequest<void>(`${basePath}/${id}/leases`, {
    method: "DELETE",
  });
}

export type PlatformRouteStateInput = Omit<ListPlatformLeasesInput, "offset"> & {
  cursor?: string;
  node_limit?: number;
  node_cursor?: string;
  node_status?: "available" | "cooling" | "circuit_open" | "not_ready" | "disabled";
};

export async function getPlatformRouteState(id: string, input: PlatformRouteStateInput = {}): Promise<PlatformRouteState> {
  const query = new URLSearchParams({
    limit: String(input.limit ?? 25),
    sort_by: input.sort_by ?? "expiry",
    sort_order: input.sort_order ?? "asc",
  });
  const account = input.account?.trim();
  if (account) {
    query.set("account", account);
    query.set("fuzzy", String(input.fuzzy ?? true));
  }
  if (input.cursor) {
    query.set("cursor", input.cursor);
  }
  query.set("node_limit", String(input.node_limit ?? 50));
  if (input.node_cursor) {
    query.set("node_cursor", input.node_cursor);
  }
  if (input.node_status) {
    query.set("node_status", input.node_status);
  }
  return apiRequest<PlatformRouteState>(`${basePath}/${id}/route-state?${query.toString()}`);
}
