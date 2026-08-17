import {
  createCooldownTemplate,
  formatResponseRules,
  normalizeResponseRuleAction,
  normalizeResponseRuleExpirySourceType,
  normalizeResponseRuleFallback,
  normalizeResponseRuleHeaderOp,
  parseResponseRulesEditorText,
  parseResponseRulesText,
  pruneRuleDraftState,
  removeRuleDraftState,
  remapRuleDraftState,
  uniqueResponseRuleID,
  validateResponseRules,
} from "./responseRulesModel.ts";
import type { PlatformResponseRule } from "./types.ts";
import { shouldResetLeaseCursorOnError } from "./routeStatePagination.ts";
import { clampPage, dataForRender, itemsForRender } from "../subscriptions/paginationModel.ts";

const assert = {
  equal(actual: unknown, expected: unknown, message?: string) {
    if (actual !== expected) throw new Error(message ?? `expected ${String(expected)}, got ${String(actual)}`);
  },
  notEqual(actual: unknown, expected: unknown, message?: string) {
    if (actual === expected) throw new Error(message ?? `did not expect ${String(expected)}`);
  },
  ok(value: unknown, message?: string) {
    if (!value) throw new Error(message ?? "expected a truthy value");
  },
  deepEqual(actual: unknown, expected: unknown, message?: string) {
    const stable = (value: unknown): string => JSON.stringify(value, (_key, nested) => {
      if (!nested || typeof nested !== "object" || Array.isArray(nested)) return nested;
      return Object.fromEntries(Object.entries(nested).sort(([left], [right]) => left.localeCompare(right)));
    });
    if (stable(actual) !== stable(expected)) throw new Error(message ?? "values differ");
  },
  match(actual: string, pattern: RegExp, message?: string) {
    if (!pattern.test(actual)) throw new Error(message ?? `value ${actual} does not match ${pattern}`);
  },
};

assert.equal(shouldResetLeaseCursorOnError(400, "stale-cursor"), true, "stale cursor should reset to page one");
assert.equal(shouldResetLeaseCursorOnError(400, ""), false, "first page 400 should not loop-reset");
assert.equal(shouldResetLeaseCursorOnError(500, "stale-cursor"), false, "server errors should remain visible");
assert.equal(clampPage(2, 21, 20), 1, "a page should clamp after the last row is deleted");
assert.equal(clampPage(0, 0, 20), 0, "an empty list should keep page zero");
assert.deepEqual(itemsForRender(["old-page-row"], true), [], "placeholder rows must not render under a new page");
assert.equal(dataForRender({ marker: "old-route-state" }, true), undefined, "placeholder route state must not render old nodes/cooldowns");
assert.equal(dataForRender({ id: "old-platform" }, true), undefined, "placeholder platform detail must not enable the old platform");

const validRule: PlatformResponseRule = {
  id: "first",
  enabled: true,
  match: { status_codes: [429], status_range: [{ min: 400, max: 499 }], body: { op: "contains", value: "quota" } },
  action: { type: "passthrough" },
};

const malformedCases = [
  "[null]",
  "[{}]",
  '[{"id":3,"enabled":true,"match":{},"action":{}}]',
  '[{"id":"x","enabled":true,"match":{"status_codes":[99]},"action":{"type":"passthrough"}}]',
  '[{"id":"x","enabled":true,"match":{"status_range":[{"min":500,"max":400}]},"action":{"type":"passthrough"}}]',
  '[{"id":"x","enabled":true,"match":{"body":{"op":"contains","value":""}},"action":{"type":"passthrough"}}]',
  '[{"id":"x","enabled":true,"match":{},"action":{"type":"passthrough","fallback":"none"}}]',
  '[{"id":"x","enabled":true,"match":{"status_codes":[429]},"action":{"type":"cooldown","cooldown_scope":"egress_ip","fallback":"none","fixed_duration":"1h"}}]',
  '[{"id":"x","enabled":true,"match":{},"action":{"type":"cooldown","cooldown_scope":"egress_ip","fallback":"none","expiry_sources":[{"type":"retry_after","format":"unix_seconds"}]}}]',
  '[{"id":"x","enabled":true,"match":{},"action":{"type":"passthrough"},"unexpected":true}]',
];
for (const input of malformedCases) {
  const parsed = parseResponseRulesText(input);
  assert.equal(parsed.rules, null, `malformed rules unexpectedly accepted: ${input}`);
  assert.ok(parsed.error);
}

const roundTrip = parseResponseRulesText(formatResponseRules([validRule]));
assert.deepEqual(roundTrip.rules, [validRule]);
assert.equal(validateResponseRules([validRule]), undefined);
assert.match(validateResponseRules([{ ...validRule, id: "" }]) ?? "", /ID/);
assert.match(validateResponseRules([{ ...validRule, match: { status_codes: [429, 429] } }]) ?? "", /重复/);

const existingRules = [validRule, { ...validRule, id: "first-2" }];
assert.equal(uniqueResponseRuleID(existingRules, "first"), "first-1");
assert.notEqual(createCooldownTemplate(existingRules).id, existingRules[0].id);

for (const input of [
  '[{"id":"empty","enabled":true,"match":{},"action":{"type":"passthrough"}}]',
  '[{"id":"empty","enabled":true,"match":{"status_codes":[]},"action":{"type":"passthrough"}}]',
]) {
  assert.equal(parseResponseRulesText(input).rules, null, `empty match unexpectedly accepted: ${input}`);
}

assert.equal(parseResponseRulesEditorText("[null, {}, {\"id\":3,\"enabled\":true,\"match\":{},\"action\":{}}]").length, 0);
assert.equal(parseResponseRulesEditorText(JSON.stringify([{
  id: "nested-malformed",
  enabled: true,
  match: { status_codes: null, status_range: { min: 400, max: 499 }, headers: [null], body: 3 },
  action: { type: "cooldown", expiry_sources: [null] },
}])).length, 0, "nested malformed rule must not reach the visual editor");
assert.equal(parseResponseRulesEditorText(JSON.stringify([{
  ...validRule,
  action: { type: "cooldown", cooldown_scope: "egress_ip", fallback: "fixed_duration", fixed_duration: "" },
}])).length, 1);

const cooldownAction: PlatformResponseRule["action"] = {
  type: "cooldown",
  cooldown_scope: "egress_ip",
  expiry_sources: [{ type: "header", header: "X-Reset", format: "unix_seconds" }],
  fallback: "fixed_duration",
  fixed_duration: "1h",
};
assert.deepEqual(normalizeResponseRuleAction("retry_next", cooldownAction), { type: "retry_next" });
assert.deepEqual(normalizeResponseRuleFallback(cooldownAction, "none"), {
  type: "cooldown",
  cooldown_scope: "egress_ip",
  expiry_sources: cooldownAction.expiry_sources,
  fallback: "none",
});
assert.deepEqual(normalizeResponseRuleHeaderOp({ name: "X-Test", op: "contains", value: "secret" }, "exists"), { name: "X-Test", op: "exists" });
assert.deepEqual(normalizeResponseRuleExpirySourceType("body_regex"), {
  type: "body_regex",
  regex: "",
  capture: 1,
  format: "rfc3339_utc",
});

const draftState = { first: "429,abc", second: "200" };
assert.deepEqual(remapRuleDraftState(draftState, "first", "renamed"), { renamed: "429,abc", second: "200" });
assert.deepEqual(removeRuleDraftState(draftState, "first"), { second: "200" });
assert.deepEqual(pruneRuleDraftState(draftState, [{ ...validRule, id: "second" }]), { second: "200" });

console.log("response rules model contracts passed");
