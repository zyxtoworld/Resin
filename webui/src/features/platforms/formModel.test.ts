import {
  defaultPlatformFormValues,
  platformToFormValues,
  toPlatformCreateInput,
  toPlatformUpdateInput,
} from "./formModel.ts";
import type { Platform } from "./types.ts";

const assert = {
  equal(actual: unknown, expected: unknown, message?: string) {
    if (actual !== expected) throw new Error(message ?? `expected ${String(expected)}, got ${String(actual)}`);
  },
  ok(value: unknown, message?: string) {
    if (!value) throw new Error(message ?? "expected a truthy value");
  },
};

const platform: Platform = {
  id: "platform-budget",
  name: "budget",
  sticky_ttl: "1h0m0s",
  proxy_request_total_timeout: "45s",
  regex_filters: [],
  region_filters: [],
  response_rules: [],
  routable_node_count: 0,
  reverse_proxy_miss_action: "TREAT_AS_EMPTY",
  reverse_proxy_empty_account_behavior: "RANDOM",
  reverse_proxy_fixed_account_header: "Authorization",
  allocation_policy: "BALANCED",
  passive_circuit_breaker_disabled: false,
  updated_at: "2026-08-21T00:00:00Z",
};

const values = platformToFormValues(platform);
assert.equal(values.proxy_request_total_timeout, "45s", "platform budget must be editable");
assert.equal(toPlatformCreateInput(values).proxy_request_total_timeout, "45s", "create must persist platform budget");
assert.equal(toPlatformUpdateInput(values).proxy_request_total_timeout, "45s", "update must persist platform budget");

const disabled = { ...defaultPlatformFormValues, name: "budget", response_rules_text: "[]" };
assert.equal(toPlatformUpdateInput(disabled).proxy_request_total_timeout, "", "empty budget must disable platform retry");
assert.ok(defaultPlatformFormValues.proxy_request_total_timeout === "", "new platforms must default to fail-closed");

console.log("platform form budget contracts passed");
