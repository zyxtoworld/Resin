import { z } from "zod";
import type { PlatformResponseRule } from "./types";

const MAX_RULES = 32;
const MAX_STATUS_CODES = 64;
const MAX_STATUS_RANGES = 16;
const MAX_HEADERS = 32;
const MAX_EXPIRY_SOURCES = 16;
const MAX_VALUE_LENGTH = 4096;

const headerNamePattern = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
const valueSchema = z.string().max(MAX_VALUE_LENGTH, "值过长");

function isValidJSONPointer(value: string): boolean {
  if (!value.startsWith("/")) return false;
  for (let index = 1; index < value.length; index += 1) {
    if (value[index] !== "~") continue;
    const escape = value[index + 1];
    if (escape !== "0" && escape !== "1") return false;
    index += 1;
  }
  return true;
}

const headerSchema = z.object({
  name: z.string().min(1, "Header 名称不能为空").max(256, "Header 名称过长").regex(headerNamePattern, "Header 名称不是合法字段名"),
  op: z.enum(["exists", "absent", "regex", "not_regex", "contains", "not_contains"]),
  value: valueSchema.optional(),
}).strict().superRefine((header, ctx) => {
  if (header.op !== "exists" && header.op !== "absent" && !header.value) {
    ctx.addIssue({ code: "custom", path: ["value"], message: "该 Header 条件需要值" });
  }
  if ((header.op === "exists" || header.op === "absent") && header.value !== undefined) {
    ctx.addIssue({ code: "custom", path: ["value"], message: `${header.op} 条件不能带值` });
  }
});

const bodySchema = z.object({
  op: z.enum(["regex", "not_regex", "contains", "not_contains"]),
  value: z.string().min(1, "响应体条件不能为空").max(MAX_VALUE_LENGTH, "响应体条件过长"),
}).strict();

const rangeSchema = z.object({
  min: z.number().int().min(100).max(599),
  max: z.number().int().min(100).max(599),
}).strict().superRefine((range, ctx) => {
  if (range.min > range.max) {
    ctx.addIssue({ code: "custom", path: ["min"], message: "状态码范围的最小值不能大于最大值" });
  }
});

const matchSchema = z.object({
  status_codes: z.array(z.number().int().min(100).max(599)).max(MAX_STATUS_CODES).optional(),
  status_range: z.array(rangeSchema).max(MAX_STATUS_RANGES).optional(),
  headers: z.array(headerSchema).max(MAX_HEADERS).optional(),
  body: bodySchema.optional(),
}).strict().superRefine((match, ctx) => {
  if (!(match.status_codes?.length || match.status_range?.length || match.headers?.length || match.body)) {
    ctx.addIssue({ code: "custom", path: [], message: "至少需要一个响应匹配条件" });
  }
  const codes = match.status_codes ?? [];
  if (new Set(codes).size !== codes.length) {
    ctx.addIssue({ code: "custom", path: ["status_codes"], message: "状态码不能重复" });
  }
});

const expirySourceSchema = z.object({
  type: z.enum(["retry_after", "header", "json_pointer", "body_regex"]),
  header: valueSchema.optional(),
  json_pointer: valueSchema.optional(),
  regex: valueSchema.optional(),
  capture: z.number().int().min(1).max(16).optional(),
  format: z.enum(["rfc3339_utc", "unix_seconds", "unix_millis", "delta_seconds"]).optional(),
}).strict().superRefine((source, ctx) => {
  const hasHeader = source.header !== undefined;
  const hasPointer = source.json_pointer !== undefined;
  const hasRegex = source.regex !== undefined;
  const hasCapture = source.capture !== undefined;
  if (source.type === "retry_after") {
    if (hasHeader || hasPointer || hasRegex || hasCapture || (source.format !== undefined && source.format !== "delta_seconds")) {
      ctx.addIssue({ code: "custom", path: ["type"], message: "Retry-After 不能带其他字段或非标准格式" });
    }
    return;
  }
  if (source.type === "header") {
    if (!source.header || !headerNamePattern.test(source.header) || hasPointer || hasRegex || hasCapture || !source.format) {
      ctx.addIssue({ code: "custom", path: ["header"], message: "Header 到期来源字段不完整或包含无关字段" });
    }
    return;
  }
  if (source.type === "json_pointer") {
    if (!source.json_pointer || !isValidJSONPointer(source.json_pointer) || hasHeader || hasRegex || hasCapture || !source.format) {
      ctx.addIssue({ code: "custom", path: ["json_pointer"], message: "JSON Pointer 到期来源字段不完整或包含无关字段" });
    }
    return;
  }
  if (!source.regex || !source.capture || hasHeader || hasPointer || !source.format) {
    ctx.addIssue({ code: "custom", path: ["regex"], message: "响应体正则来源字段不完整或包含无关字段" });
  }
});

const actionSchema = z.object({
  type: z.enum(["passthrough", "retry_next", "cooldown", "cooldown_then_retry_next"]),
  cooldown_scope: z.enum(["egress_ip", "route_entry"]).optional(),
  expiry_sources: z.array(expirySourceSchema).max(MAX_EXPIRY_SOURCES).optional(),
  fallback: z.enum(["next_utc_midnight", "fixed_duration", "none"]).optional(),
  fixed_duration: valueSchema.optional(),
}).strict().superRefine((action, ctx) => {
  const cooldown = action.type === "cooldown" || action.type === "cooldown_then_retry_next";
  if (!cooldown && (action.cooldown_scope !== undefined || action.expiry_sources !== undefined || action.fallback !== undefined || action.fixed_duration !== undefined)) {
    ctx.addIssue({ code: "custom", path: ["type"], message: "非冷却动作不能带冷却参数" });
  }
  if (cooldown && !action.cooldown_scope) {
    ctx.addIssue({ code: "custom", path: ["cooldown_scope"], message: "冷却动作需要作用域" });
  }
  if (cooldown && !action.fallback) {
    ctx.addIssue({ code: "custom", path: ["fallback"], message: "冷却动作需要兜底策略" });
  }
  if (action.fallback === "fixed_duration" && !action.fixed_duration) {
    ctx.addIssue({ code: "custom", path: ["fixed_duration"], message: "固定时长不能为空" });
  }
  if (action.fallback !== "fixed_duration" && action.fixed_duration !== undefined) {
    ctx.addIssue({ code: "custom", path: ["fixed_duration"], message: "非固定时长兜底不能带 fixed_duration" });
  }
});

const responseRuleSchema = z.object({
  id: z.string().min(1, "规则 ID 不能为空").max(128, "规则 ID 不能超过 128 个字符"),
  enabled: z.boolean(),
  match: matchSchema,
  action: actionSchema,
}).strict();

const responseRulesSchema = z.array(responseRuleSchema).max(MAX_RULES, `规则数量不能超过 ${MAX_RULES}`);

export type ResponseRuleHeader = NonNullable<PlatformResponseRule["match"]["headers"]>[number];
export type ResponseRuleExpirySource = NonNullable<NonNullable<PlatformResponseRule["action"]["expiry_sources"]>[number]>;

export function normalizeResponseRuleAction(
  type: PlatformResponseRule["action"]["type"],
  current: PlatformResponseRule["action"],
): PlatformResponseRule["action"] {
  if (type === "passthrough" || type === "retry_next") {
    return { type };
  }
  return {
    type,
    cooldown_scope: current.cooldown_scope ?? "egress_ip",
    expiry_sources: current.expiry_sources ?? [],
    fallback: current.fallback ?? "next_utc_midnight",
    ...(current.fallback === "fixed_duration" && current.fixed_duration ? { fixed_duration: current.fixed_duration } : {}),
  };
}

export function normalizeResponseRuleFallback(
  current: PlatformResponseRule["action"],
  fallback: NonNullable<PlatformResponseRule["action"]["fallback"]>,
): PlatformResponseRule["action"] {
  const normalized: PlatformResponseRule["action"] = {
    type: current.type,
    cooldown_scope: current.cooldown_scope,
    expiry_sources: current.expiry_sources,
    fallback,
  };
  if (fallback === "fixed_duration") {
    normalized.fixed_duration = current.fixed_duration ?? "";
  }
  return normalized;
}

export function normalizeResponseRuleHeaderOp(header: ResponseRuleHeader, op: ResponseRuleHeader["op"]): ResponseRuleHeader {
  if (op === "exists" || op === "absent") {
    return { name: header.name, op };
  }
  return { name: header.name, op, value: header.value ?? "" };
}

export function normalizeResponseRuleExpirySourceType(type: ResponseRuleExpirySource["type"]): ResponseRuleExpirySource {
  if (type === "retry_after") {
    return { type };
  }
  if (type === "header") {
    return { type, header: "", format: "delta_seconds" };
  }
  if (type === "json_pointer") {
    return { type, json_pointer: "", format: "rfc3339_utc" };
  }
  return { type, regex: "", capture: 1, format: "rfc3339_utc" };
}

/**
 * Retry-After has its own HTTP semantics: the server may send delta-seconds
 * or an HTTP-date. Keep the legacy delta_seconds field readable, but never
 * write it back from the visual editor.
 */
export function normalizeResponseRuleExpirySource(source: ResponseRuleExpirySource): ResponseRuleExpirySource {
  return source.type === "retry_after" ? { type: "retry_after" } : { ...source };
}

export function normalizeResponseRules(rules: PlatformResponseRule[]): PlatformResponseRule[] {
  return rules.map((rule) => {
    const sources = rule.action.expiry_sources;
    if (!sources) {
      return rule;
    }
    return {
      ...rule,
      action: {
        ...rule.action,
        expiry_sources: sources.map(normalizeResponseRuleExpirySource),
      },
    };
  });
}

export function moveResponseRuleExpirySource(
  sources: ResponseRuleExpirySource[],
  index: number,
  direction: -1 | 1,
): ResponseRuleExpirySource[] {
  const target = index + direction;
  if (index < 0 || target < 0 || index >= sources.length || target >= sources.length) {
    return sources;
  }
  const next = sources.map(normalizeResponseRuleExpirySource);
  [next[index], next[target]] = [next[target], next[index]];
  return next;
}

export function parseResponseRulesText(text: string): { rules: PlatformResponseRule[] | null; error?: string } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text.trim() || "[]");
  } catch {
    return { rules: null, error: "响应规则 JSON 格式无效" };
  }
  const result = responseRulesSchema.safeParse(parsed);
  if (!result.success) {
    return { rules: null, error: result.error.issues[0]?.message ?? "响应规则结构无效" };
  }
  return { rules: normalizeResponseRules(result.data as PlatformResponseRule[]) };
}

function isEditorRule(value: unknown): value is PlatformResponseRule {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  if (typeof candidate.id !== "string" || typeof candidate.enabled !== "boolean") {
    return false;
  }
  if (!isEditorMatch(candidate.match) || !isEditorAction(candidate.action)) {
    return false;
  }
  return true;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isOneOf<T extends string>(value: unknown, values: readonly T[]): value is T {
  return typeof value === "string" && values.includes(value as T);
}

const editorHeaderOps = ["exists", "absent", "regex", "not_regex", "contains", "not_contains"] as const;
const editorBodyOps = ["regex", "not_regex", "contains", "not_contains"] as const;
const editorExpiryTypes = ["retry_after", "header", "json_pointer", "body_regex"] as const;
const editorExpiryFormats = ["rfc3339_utc", "unix_seconds", "unix_millis", "delta_seconds"] as const;
const editorActionTypes = ["passthrough", "retry_next", "cooldown", "cooldown_then_retry_next"] as const;
const editorCooldownScopes = ["egress_ip", "route_entry"] as const;
const editorFallbacks = ["next_utc_midnight", "fixed_duration", "none"] as const;

function isOptionalString(value: unknown): boolean {
  return value === undefined || typeof value === "string";
}

function isEditorHeader(value: unknown): boolean {
  if (!isRecord(value) || typeof value.name !== "string" || !isOneOf(value.op, editorHeaderOps)) {
    return false;
  }
  return isOptionalString(value.value);
}

function isEditorBody(value: unknown): boolean {
  return isRecord(value)
    && isOneOf(value.op, editorBodyOps)
    && typeof value.value === "string";
}

function isEditorRange(value: unknown): boolean {
  return isRecord(value) && typeof value.min === "number" && typeof value.max === "number";
}

function isEditorMatch(value: unknown): value is PlatformResponseRule["match"] {
  if (!isRecord(value)) {
    return false;
  }
  if (value.status_codes !== undefined && (!Array.isArray(value.status_codes) || !value.status_codes.every((item) => typeof item === "number"))) {
    return false;
  }
  if (value.status_range !== undefined && (!Array.isArray(value.status_range) || !value.status_range.every(isEditorRange))) {
    return false;
  }
  if (value.headers !== undefined && (!Array.isArray(value.headers) || !value.headers.every(isEditorHeader))) {
    return false;
  }
  return value.body === undefined || isEditorBody(value.body);
}

function isEditorExpirySource(value: unknown): boolean {
  if (!isRecord(value) || !isOneOf(value.type, editorExpiryTypes)) {
    return false;
  }
  if (!isOptionalString(value.header) || !isOptionalString(value.json_pointer) || !isOptionalString(value.regex)) {
    return false;
  }
  if (value.capture !== undefined && typeof value.capture !== "number") {
    return false;
  }
  if (value.type === "retry_after" && value.format !== undefined && value.format !== "delta_seconds") {
    return false;
  }
  return value.format === undefined || isOneOf(value.format, editorExpiryFormats);
}

function isEditorAction(value: unknown): value is PlatformResponseRule["action"] {
  if (!isRecord(value) || !isOneOf(value.type, editorActionTypes)) {
    return false;
  }
  if (value.cooldown_scope !== undefined && !isOneOf(value.cooldown_scope, editorCooldownScopes)) {
    return false;
  }
  if (value.expiry_sources !== undefined && (!Array.isArray(value.expiry_sources) || !value.expiry_sources.every(isEditorExpirySource))) {
    return false;
  }
  if (value.fallback !== undefined && !isOneOf(value.fallback, editorFallbacks)) {
    return false;
  }
  return isOptionalString(value.fixed_duration);
}

/**
 * Keep structurally editable rules while the visual editor repairs an invalid
 * discriminant variant. Submission and advanced import still use the strict
 * parser above; this parser only prevents a bad intermediate field from
 * replacing the whole editor state with an empty list.
 */
export function parseResponseRulesEditorText(text: string): PlatformResponseRule[] {
  try {
    const parsed: unknown = JSON.parse(text.trim() || "[]");
    return Array.isArray(parsed) ? normalizeResponseRules(parsed.filter(isEditorRule)) : [];
  } catch {
    return [];
  }
}

export function formatResponseRules(rules: PlatformResponseRule[]): string {
  return JSON.stringify(normalizeResponseRules(rules), null, 2);
}

export function cloneResponseRule(rule: PlatformResponseRule): PlatformResponseRule {
  return JSON.parse(JSON.stringify(rule)) as PlatformResponseRule;
}

export function uniqueResponseRuleID(rules: PlatformResponseRule[], prefix: string): string {
  const used = new Set(rules.map((rule) => rule.id));
  let index = 1;
  let candidate = `${prefix}-${index}`;
  while (used.has(candidate)) {
    index += 1;
    candidate = `${prefix}-${index}`;
  }
  return candidate;
}

export function remapRuleDraftState(state: Record<string, string>, previousID: string, nextID: string): Record<string, string> {
  if (previousID === nextID || state[previousID] === undefined) {
    return state;
  }
  const next = { ...state };
  next[nextID] = next[previousID];
  delete next[previousID];
  return next;
}

export function removeRuleDraftState(state: Record<string, string>, ruleID: string): Record<string, string> {
  if (state[ruleID] === undefined) {
    return state;
  }
  const next = { ...state };
  delete next[ruleID];
  return next;
}

export function pruneRuleDraftState(state: Record<string, string>, rules: PlatformResponseRule[]): Record<string, string> {
  const active = new Set(rules.map((rule) => rule.id));
  return Object.fromEntries(Object.entries(state).filter(([ruleID]) => active.has(ruleID)));
}

export function createResponseRule(rules: PlatformResponseRule[]): PlatformResponseRule {
  return {
    id: uniqueResponseRuleID(rules, "response-rule"),
    enabled: true,
    match: { status_codes: [429] },
    action: { type: "passthrough" },
  };
}

export function createCooldownTemplate(rules: PlatformResponseRule[]): PlatformResponseRule {
  return {
    id: uniqueResponseRuleID(rules, "quota-cooldown"),
    enabled: true,
    match: { status_codes: [429] },
    action: {
      type: "cooldown_then_retry_next",
      cooldown_scope: "egress_ip",
      expiry_sources: [{ type: "retry_after" }],
      fallback: "next_utc_midnight",
    },
  };
}

export function validateResponseRules(rules: PlatformResponseRule[]): string | undefined {
  const result = responseRulesSchema.safeParse(rules);
  if (!result.success) {
    return result.error.issues[0]?.message ?? "响应规则结构无效";
  }
  const ids = new Set<string>();
  for (const [index, rule] of rules.entries()) {
    const id = rule.id.trim();
    if (ids.has(id)) {
      return `第 ${index + 1} 条规则的 ID 重复：${id}`;
    }
    ids.add(id);
  }
  return undefined;
}
