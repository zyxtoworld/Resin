import { z } from "zod";
import { allocationPolicies, emptyAccountBehaviors, missActions } from "./constants.ts";
import { parseHeaderLines, parseLinesToList } from "./formParsers.ts";
import { parseResponseRulesText } from "./responseRulesModel.ts";
import type { Platform, PlatformCreateInput, PlatformResponseRule, PlatformUpdateInput } from "./types";

const platformNameForbiddenChars = ".:|/\\@?#%~";
const platformNameForbiddenSpacing = " \t\r\n";
const platformNameReserved = "api";

function containsAny(source: string, chars: string): boolean {
  for (const ch of chars) {
    if (source.includes(ch)) {
      return true;
    }
  }
  return false;
}

export const platformNameRuleHint = "平台名不能包含 .:|/\\@?#%~、空格、Tab、换行、回车，也不能为保留字。";

export const platformFormSchema = z.object({
  name: z.string().trim()
    .min(1, "平台名称不能为空")
    .refine((value) => !containsAny(value, platformNameForbiddenChars), {
      message: "平台名称不能包含字符 .:|/\\@?#%~",
    })
    .refine((value) => !containsAny(value, platformNameForbiddenSpacing), {
      message: "平台名称不能包含空格、Tab、换行、回车",
    })
    .refine((value) => value.toLowerCase() !== platformNameReserved, {
      message: "平台名称不能为保留字",
    }),
  sticky_ttl: z.string().optional(),
  proxy_request_total_timeout: z.string().optional(),
  proxy_request_attempt_timeout: z.string().optional(),
  proxy_request_max_attempts: z.number().int().min(0).optional(),
  regex_filters_text: z.string().optional(),
  region_filters_text: z.string().optional(),
  response_rules_text: z.string(),
  reverse_proxy_miss_action: z.enum(missActions),
  reverse_proxy_empty_account_behavior: z.enum(emptyAccountBehaviors),
  reverse_proxy_fixed_account_header: z.string().optional(),
  allocation_policy: z.enum(allocationPolicies),
  passive_circuit_breaker_disabled: z.boolean(),
}).superRefine((value, ctx) => {
  if (
    value.reverse_proxy_empty_account_behavior === "FIXED_HEADER" &&
    parseHeaderLines(value.reverse_proxy_fixed_account_header).length === 0
  ) {
    ctx.addIssue({
      code: "custom",
      path: ["reverse_proxy_fixed_account_header"],
      message: "用于提取 Account 的 Headers 不能为空",
    });
  }

  const parsed = parseResponseRulesText(value.response_rules_text);
  if (!parsed.rules) {
    ctx.addIssue({ code: "custom", path: ["response_rules_text"], message: parsed.error ?? "响应规则结构无效" });
  }
});

export type PlatformFormValues = z.infer<typeof platformFormSchema>;

export const defaultPlatformFormValues: PlatformFormValues = {
  name: "",
  sticky_ttl: "",
  proxy_request_total_timeout: "",
  proxy_request_attempt_timeout: "",
  proxy_request_max_attempts: 0,
  regex_filters_text: "",
  region_filters_text: "",
  response_rules_text: "",
  reverse_proxy_miss_action: "TREAT_AS_EMPTY",
  reverse_proxy_empty_account_behavior: "RANDOM",
  reverse_proxy_fixed_account_header: "Authorization",
  allocation_policy: "BALANCED",
  passive_circuit_breaker_disabled: false,
};

export function platformToFormValues(platform: Platform): PlatformFormValues {
  const regexFilters = Array.isArray(platform.regex_filters) ? platform.regex_filters : [];
  const regionFilters = Array.isArray(platform.region_filters) ? platform.region_filters : [];

  return {
    name: platform.name,
    sticky_ttl: platform.sticky_ttl,
    proxy_request_total_timeout: platform.proxy_request_total_timeout ?? "",
    proxy_request_attempt_timeout: platform.proxy_request_attempt_timeout ?? "",
    proxy_request_max_attempts: platform.proxy_request_max_attempts ?? 0,
    regex_filters_text: regexFilters.join("\n"),
    region_filters_text: regionFilters.join("\n"),
    response_rules_text: JSON.stringify(platform.response_rules ?? [], null, 2),
    reverse_proxy_miss_action: platform.reverse_proxy_miss_action,
    reverse_proxy_empty_account_behavior: platform.reverse_proxy_empty_account_behavior,
    reverse_proxy_fixed_account_header: platform.reverse_proxy_fixed_account_header,
    allocation_policy: platform.allocation_policy,
    passive_circuit_breaker_disabled: platform.passive_circuit_breaker_disabled,
  };
}

function toPlatformPayloadBase(values: PlatformFormValues) {
  const responseRules = JSON.parse(values.response_rules_text || "[]") as PlatformResponseRule[];
  return {
    name: values.name.trim(),
    proxy_request_total_timeout: values.proxy_request_total_timeout?.trim() ?? "",
    proxy_request_attempt_timeout: values.proxy_request_attempt_timeout?.trim() ?? "",
    proxy_request_max_attempts: values.proxy_request_max_attempts ?? 0,
    regex_filters: parseLinesToList(values.regex_filters_text),
    region_filters: parseLinesToList(values.region_filters_text, (value) => value.toLowerCase()),
    response_rules: responseRules,
    reverse_proxy_miss_action: values.reverse_proxy_miss_action,
    reverse_proxy_empty_account_behavior: values.reverse_proxy_empty_account_behavior,
    reverse_proxy_fixed_account_header: parseHeaderLines(values.reverse_proxy_fixed_account_header).join("\n"),
    allocation_policy: values.allocation_policy,
    passive_circuit_breaker_disabled: values.passive_circuit_breaker_disabled,
  };
}

export function toPlatformCreateInput(values: PlatformFormValues): PlatformCreateInput {
  return {
    ...toPlatformPayloadBase(values),
    sticky_ttl: values.sticky_ttl?.trim() || undefined,
    proxy_request_total_timeout: values.proxy_request_total_timeout?.trim() || undefined,
    proxy_request_attempt_timeout: values.proxy_request_attempt_timeout?.trim() || undefined,
    proxy_request_max_attempts: values.proxy_request_max_attempts || undefined,
  };
}

export function toPlatformUpdateInput(values: PlatformFormValues): PlatformUpdateInput {
  return {
    ...toPlatformPayloadBase(values),
    sticky_ttl: values.sticky_ttl?.trim() || "",
    proxy_request_total_timeout: values.proxy_request_total_timeout?.trim() || "",
    proxy_request_attempt_timeout: values.proxy_request_attempt_timeout?.trim() || "",
    proxy_request_max_attempts: values.proxy_request_max_attempts ?? 0,
  };
}
