import { ChevronDown, ChevronUp, Code2, Copy, Plus, Trash2 } from "lucide-react";
import { useEffect, useState } from "react";
import { Button } from "../../components/ui/Button";
import { Input } from "../../components/ui/Input";
import { Select } from "../../components/ui/Select";
import { Switch } from "../../components/ui/Switch";
import { Textarea } from "../../components/ui/Textarea";
import { useI18n } from "../../i18n";
import {
  cloneResponseRule,
  createCooldownTemplate,
  createResponseRule,
  formatResponseRules,
  moveResponseRuleExpirySource,
  normalizeResponseRuleAction,
  normalizeResponseRuleExpirySourceType,
  normalizeResponseRuleFallback,
  normalizeResponseRuleHeaderOp,
  parseResponseRulesText,
  removeRuleDraftState,
  type ResponseRuleExpirySource,
  type ResponseRuleHeader,
  uniqueResponseRuleID,
  validateResponseRules,
} from "./responseRulesModel";
import type { PlatformResponseRule } from "./types";

type ResponseRulesEditorProps = {
  rules: PlatformResponseRule[];
  onChange: (rules: PlatformResponseRule[]) => void;
  onValidationChange?: (message?: string) => void;
  error?: string;
};

const actionNeedsCooldown = (type: PlatformResponseRule["action"]["type"]) =>
  type === "cooldown" || type === "cooldown_then_retry_next";

const sourceTypes: ResponseRuleExpirySource["type"][] = ["retry_after", "header", "json_pointer", "body_regex"];
const sourceFormats: NonNullable<ResponseRuleExpirySource["format"]>[] = [
  "rfc3339_utc",
  "unix_seconds",
  "unix_millis",
  "delta_seconds",
];

let nextResponseRuleRowKey = 0;

function newResponseRuleRowKey(): string {
  nextResponseRuleRowKey += 1;
  return `response-rule-row-${nextResponseRuleRowKey}`;
}

function parseNumberList(value: string): { values?: number[]; error?: string } {
  const raw = value.trim();
  if (!raw) {
    return {};
  }
  const tokens = raw.split(",").map((item) => item.trim());
  const numbers = tokens.map((token) => Number(token));
  if (tokens.some((token, index) => !token || !Number.isInteger(numbers[index]) || numbers[index] < 100 || numbers[index] > 599)) {
    return { error: "状态码必须是 100-599 的整数，多个值用逗号分隔" };
  }
  if (new Set(numbers).size !== numbers.length) {
    return { error: "状态码不能重复" };
  }
  return { values: numbers };
}

function statusCodesValue(rule: PlatformResponseRule): string {
  return (rule.match.status_codes ?? []).join(", ");
}

function updateHeader(header: ResponseRuleHeader, key: keyof ResponseRuleHeader, value: string): ResponseRuleHeader {
  if (key === "op") {
    return normalizeResponseRuleHeaderOp(header, value as ResponseRuleHeader["op"]);
  }
  return { ...header, [key]: value };
}

function updateSource(source: ResponseRuleExpirySource, key: keyof ResponseRuleExpirySource, value: string): ResponseRuleExpirySource {
  if (key === "type") {
    return normalizeResponseRuleExpirySourceType(value as ResponseRuleExpirySource["type"]);
  }
  if (key === "format") {
    return { ...source, format: value as NonNullable<ResponseRuleExpirySource["format"]> };
  }
  if (key === "capture") {
    const capture = Number(value);
    return Number.isInteger(capture) ? { ...source, capture } : { ...source, capture: undefined };
  }
  return { ...source, [key]: value };
}

const sourceTypeLabels: Record<ResponseRuleExpirySource["type"], string> = {
  retry_after: "标准 Retry-After 响应头",
  header: "指定响应头",
  json_pointer: "响应体 JSON 字段",
  body_regex: "响应正文正则捕获",
};

const sourceFormatLabels: Record<NonNullable<ResponseRuleExpirySource["format"]>, string> = {
  delta_seconds: "从现在起的秒数",
  unix_seconds: "Unix 时间戳（秒）",
  unix_millis: "Unix 时间戳（毫秒）",
  rfc3339_utc: "UTC 日期时间（RFC3339）",
};

export function ResponseRulesEditor({ rules, onChange, onValidationChange, error }: ResponseRulesEditorProps) {
  const { t } = useI18n();
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [advancedText, setAdvancedText] = useState(() => formatResponseRules(rules));
  const [advancedError, setAdvancedError] = useState<string>();
  const [statusDrafts, setStatusDrafts] = useState<Record<string, string>>({});
  const [statusErrors, setStatusErrors] = useState<Record<string, string>>({});
  const [rowKeys, setRowKeys] = useState<string[]>(() => rules.map(() => newResponseRuleRowKey()));
  const ruleCount = rules.length;
  useEffect(() => {
    // Controlled platform data can arrive after the editor mounts. This is a
    // local identity list, not business state; synchronize only its length.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setRowKeys((current) => {
      if (current.length === ruleCount) {
        return current;
      }
      if (current.length > ruleCount) {
        return current.slice(0, ruleCount);
      }
      return [...current, ...Array.from({ length: ruleCount - current.length }, () => newResponseRuleRowKey())];
    });
  }, [ruleCount]);
  const validationError = validateResponseRules(rules);

  const updateAt = (index: number, update: (rule: PlatformResponseRule) => PlatformResponseRule) => {
    const next = rules.map((rule, ruleIndex) => (ruleIndex === index ? update(cloneResponseRule(rule)) : rule));
    onChange(next);
  };

  const moveRule = (index: number, direction: -1 | 1) => {
    const target = index + direction;
    if (target < 0 || target >= rules.length) {
      return;
    }
    const next = [...rules];
    [next[index], next[target]] = [next[target], next[index]];
    setRowKeys((current) => {
      const nextKeys = [...current];
      [nextKeys[index], nextKeys[target]] = [nextKeys[target], nextKeys[index]];
      return nextKeys;
    });
    onChange(next);
  };

  const addRule = () => {
    setRowKeys((current) => [...current, newResponseRuleRowKey()]);
    onChange([...rules, createResponseRule(rules)]);
  };

  const addTemplate = () => {
    setRowKeys((current) => [...current, newResponseRuleRowKey()]);
    onChange([...rules, createCooldownTemplate(rules)]);
  };

  const copyRule = (index: number) => {
    const rule = rules[index];
    if (!rule) {
      return;
    }
    setRowKeys((current) => [...current.slice(0, index + 1), newResponseRuleRowKey(), ...current.slice(index + 1)]);
    onChange([
      ...rules.slice(0, index + 1),
      { ...cloneResponseRule(rule), id: uniqueResponseRuleID(rules, `${rule.id}-copy`) },
      ...rules.slice(index + 1),
    ]);
  };

  const removeRule = (index: number) => {
    const rowKey = rowKeys[index];
    if (rowKey) {
      setStatusDrafts((current) => removeRuleDraftState(current, rowKey));
      setStatusErrors((current) => removeRuleDraftState(current, rowKey));
    }
    setRowKeys((current) => current.filter((_, rowIndex) => rowIndex !== index));
    onChange(rules.filter((_, ruleIndex) => ruleIndex !== index));
  };

  const moveExpirySource = (ruleIndex: number, sourceIndex: number, direction: -1 | 1) => {
    updateAt(ruleIndex, (current) => ({
      ...current,
      action: {
        ...current.action,
        expiry_sources: moveResponseRuleExpirySource(current.action.expiry_sources ?? [], sourceIndex, direction),
      },
    }));
  };

  const applyAdvanced = () => {
    const parsed = parseResponseRulesText(advancedText);
    if (!parsed.rules) {
      setAdvancedError(parsed.error);
      onValidationChange?.(parsed.error);
      return;
    }
    const validation = validateResponseRules(parsed.rules);
    if (validation) {
      setAdvancedError(validation);
      onValidationChange?.(validation);
      return;
    }
    setAdvancedError(undefined);
    setStatusDrafts({});
    setStatusErrors({});
    onValidationChange?.(undefined);
    setRowKeys(parsed.rules.map(() => newResponseRuleRowKey()));
    onChange(parsed.rules);
  };

  return (
    <div className="response-rules-editor">
      <div className="response-rules-toolbar">
        <div>
          <strong>{t("响应规则")}</strong>
          <p className="muted">{t("按列表顺序 first-match-wins；保存时仍由后端编译器做最终校验。")}</p>
        </div>
        <div className="response-rules-toolbar-actions">
          <Button variant="secondary" size="sm" onClick={addRule}>
            <Plus size={14} /> {t("添加规则")}
          </Button>
          <Button variant="secondary" size="sm" onClick={addTemplate}>
            <Plus size={14} /> {t("添加冷却模板")}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              setAdvancedText(formatResponseRules(rules));
              setAdvancedError(undefined);
              setAdvancedOpen((open) => !open);
            }}
          >
            <Code2 size={14} /> {advancedOpen ? t("收起高级 JSON") : t("高级导入/导出")}
          </Button>
        </div>
      </div>

      {validationError || error ? <p className="field-error">{error ?? validationError}</p> : null}

      {advancedOpen ? (
        <div className="response-rules-advanced">
          <Textarea
            rows={10}
            value={advancedText}
            onChange={(event) => setAdvancedText(event.target.value)}
            aria-label={t("响应规则高级 JSON")}
          />
          <div className="response-rules-advanced-actions">
            <Button variant="secondary" size="sm" onClick={() => setAdvancedText(formatResponseRules(rules))}>
              {t("导出当前规则")}
            </Button>
            <Button size="sm" onClick={applyAdvanced}>{t("应用 JSON")}</Button>
          </div>
          {advancedError ? <p className="field-error">{advancedError}</p> : null}
        </div>
      ) : null}

      {!rules.length ? <p className="empty-box">{t("尚未配置响应规则。")}</p> : null}

      <div className="response-rules-list">
        {rules.map((rule, index) => {
          const rowKey = rowKeys[index] ?? `response-rule-row-pending-${index}`;
          const statusDraftKey = rowKey;
          const action = rule.action;
          const cooldown = actionNeedsCooldown(action.type);
          const failureRule = Boolean(rule.match.failure_kinds?.length);
          const headers = rule.match.headers ?? [];
          const sources = action.expiry_sources ?? [];
          return (
            <article className="response-rule-card" key={rowKey}>
              <header className="response-rule-card-header">
                <div className="response-rule-card-title">
                  <span className="response-rule-order">{index + 1}</span>
                  <Input
                    aria-label={t("规则 ID")}
                    value={rule.id}
                    onChange={(event) => updateAt(index, (current) => ({ ...current, id: event.target.value }))}
                  />
                  <label className="response-rule-enabled">
                    <Switch
                      checked={rule.enabled}
                      onChange={(event) => updateAt(index, (current) => ({ ...current, enabled: event.target.checked }))}
                    />
                    {t("启用")}
                  </label>
                </div>
                <div className="response-rule-card-actions">
                  <Button variant="ghost" size="sm" onClick={() => moveRule(index, -1)} disabled={index === 0} aria-label={t("上移")}> <ChevronUp size={15} /> </Button>
                  <Button variant="ghost" size="sm" onClick={() => moveRule(index, 1)} disabled={index === rules.length - 1} aria-label={t("下移")}> <ChevronDown size={15} /> </Button>
                  <Button variant="ghost" size="sm" onClick={() => copyRule(index)} aria-label={t("复制")}> <Copy size={15} /> </Button>
                  <Button variant="ghost" size="sm" onClick={() => removeRule(index)} aria-label={t("删除")} style={{ color: "var(--delete-btn-color, #c27070)" }}> <Trash2 size={15} /> </Button>
                </div>
              </header>

              <div className="response-rule-card-grid">
                <div className="response-rule-section">
                  <h5>{t("匹配条件")}</h5>
                  <label className="field-label">{t("状态码（逗号分隔）")}</label>
                  <Input
                    value={statusDrafts[statusDraftKey] ?? statusCodesValue(rule)}
                    placeholder="200, 429"
                    onChange={(event) => {
                      const raw = event.target.value;
                      const parsed = parseNumberList(raw);
                      setStatusDrafts((current) => ({ ...current, [statusDraftKey]: raw }));
                      const nextStatusErrors = { ...statusErrors, [statusDraftKey]: parsed.error ?? "" };
                      setStatusErrors(nextStatusErrors);
                      onValidationChange?.(Object.values(nextStatusErrors).find(Boolean));
                      if (!parsed.error) {
                        updateAt(index, (current) => ({ ...current, match: { ...current.match, status_codes: parsed.values } }));
                      }
                    }}
                  />
                  {statusErrors[statusDraftKey] ? <p className="field-error">{statusErrors[statusDraftKey]}</p> : null}
                  <label className="field-label">{t("状态码范围")}</label>
                  {(rule.match.status_range ?? []).map((range, rangeIndex) => (
                    <div className="response-rule-inline" key={`${range.min}-${range.max}-${rangeIndex}`}>
                      <Input type="number" value={range.min} onChange={(event) => updateAt(index, (current) => ({ ...current, match: { ...current.match, status_range: (current.match.status_range ?? []).map((item, itemIndex) => itemIndex === rangeIndex ? { ...item, min: Number(event.target.value) } : item) } }))} />
                      <span>—</span>
                      <Input type="number" value={range.max} onChange={(event) => updateAt(index, (current) => ({ ...current, match: { ...current.match, status_range: (current.match.status_range ?? []).map((item, itemIndex) => itemIndex === rangeIndex ? { ...item, max: Number(event.target.value) } : item) } }))} />
                      <Button variant="ghost" size="sm" onClick={() => updateAt(index, (current) => ({ ...current, match: { ...current.match, status_range: (current.match.status_range ?? []).filter((_, itemIndex) => itemIndex !== rangeIndex) } }))}><Trash2 size={14} /></Button>
                    </div>
                  ))}
                  <Button variant="ghost" size="sm" onClick={() => updateAt(index, (current) => ({ ...current, match: { ...current.match, status_range: [...(current.match.status_range ?? []), { min: 400, max: 499 }] } }))}><Plus size={14} /> {t("添加范围")}</Button>

                  <div className="response-rule-subsection">
                    <div className="response-rule-subsection-head"><strong>{t("Header 条件")}</strong><Button variant="ghost" size="sm" onClick={() => updateAt(index, (current) => ({ ...current, match: { ...current.match, headers: [...(current.match.headers ?? []), { name: "", op: "exists" }] } }))}><Plus size={14} /></Button></div>
                    {headers.map((header, headerIndex) => (
                      <div className="response-rule-condition" key={`${header.name}-${headerIndex}`}>
                        <Input placeholder={t("字段名")} value={header.name} onChange={(event) => updateAt(index, (current) => ({ ...current, match: { ...current.match, headers: (current.match.headers ?? []).map((item, itemIndex) => itemIndex === headerIndex ? updateHeader(item, "name", event.target.value) : item) } }))} />
                        <Select value={header.op} onChange={(event) => updateAt(index, (current) => ({ ...current, match: { ...current.match, headers: (current.match.headers ?? []).map((item, itemIndex) => itemIndex === headerIndex ? updateHeader(item, "op", event.target.value) : item) } }))}>
                          {(["exists", "absent", "regex", "not_regex", "contains", "not_contains"] as const).map((op) => <option key={op} value={op}>{op}</option>)}
                        </Select>
                        {header.op !== "exists" && header.op !== "absent" ? <Input placeholder={t("匹配值")} value={header.value ?? ""} onChange={(event) => updateAt(index, (current) => ({ ...current, match: { ...current.match, headers: (current.match.headers ?? []).map((item, itemIndex) => itemIndex === headerIndex ? updateHeader(item, "value", event.target.value) : item) } }))} /> : null}
                        <Button variant="ghost" size="sm" onClick={() => updateAt(index, (current) => ({ ...current, match: { ...current.match, headers: (current.match.headers ?? []).filter((_, itemIndex) => itemIndex !== headerIndex) } }))}><Trash2 size={14} /></Button>
                      </div>
                    ))}
                  </div>

                  <div className="response-rule-subsection">
                    <strong>{t("响应体条件")}</strong>
                    {rule.match.body ? (
                      <div className="response-rule-condition">
                        <Select value={rule.match.body.op} onChange={(event) => updateAt(index, (current) => ({ ...current, match: { ...current.match, body: { ...current.match.body!, op: event.target.value as NonNullable<PlatformResponseRule["match"]["body"]>["op"] } } }))}>
                          {(["regex", "not_regex", "contains", "not_contains"] as const).map((op) => <option key={op} value={op}>{op}</option>)}
                        </Select>
                        <Input value={rule.match.body.value} onChange={(event) => updateAt(index, (current) => ({ ...current, match: { ...current.match, body: { ...current.match.body!, value: event.target.value } } }))} />
                        <Button variant="ghost" size="sm" onClick={() => updateAt(index, (current) => ({ ...current, match: { ...current.match, body: undefined } }))}><Trash2 size={14} /></Button>
                      </div>
                    ) : <Button variant="ghost" size="sm" onClick={() => updateAt(index, (current) => ({ ...current, match: { ...current.match, body: { op: "contains", value: "" } } }))}><Plus size={14} /> {t("添加响应体条件")}</Button>}
                  </div>
                </div>

                <div className="response-rule-section">
                  <h5>{t("动作")}</h5>
                  <Select value={action.type} onChange={(event) => updateAt(index, (current) => ({ ...current, action: normalizeResponseRuleAction(event.target.value as PlatformResponseRule["action"]["type"], current.action) }))}>
                    <option value="passthrough">{t("透传")}</option>
                    <option value="retry_next">{t("重试下个")}</option>
                    <option value="cooldown">{t("冷却")}</option>
                    <option value="cooldown_then_retry_next">{t("冷却并重试")}</option>
                  </Select>
                  {cooldown ? (
                    <>
                      <label className="field-label">{t("冷却作用域")}</label>
                      <Select value={action.cooldown_scope ?? "egress_ip"} onChange={(event) => updateAt(index, (current) => ({ ...current, action: { ...current.action, cooldown_scope: event.target.value as NonNullable<PlatformResponseRule["action"]["cooldown_scope"]> } }))}>
                        <option value="egress_ip">{t("出口 IP")}</option>
                        <option value="route_entry">{t("当前路由节点")}</option>
                      </Select>
                      {failureRule ? <p className="muted">{t("失败类别没有响应头或响应体，到期时间只能使用兜底策略。")}</p> : <>
                      <div className="response-rule-subsection-head">
                        <div>
                          <strong>{t("冷却到期时间（按优先级尝试）")}</strong>
                          <p className="muted">{t("依次从响应中读取到期时间，首个可解析值生效；都取不到时使用下方兜底策略。")}</p>
                        </div>
                        <Button variant="ghost" size="sm" aria-label={t("添加到期来源")} onClick={() => updateAt(index, (current) => ({ ...current, action: { ...current.action, expiry_sources: [...(current.action.expiry_sources ?? []), { type: "retry_after" }] } }))}><Plus size={14} /></Button>
                      </div>
                      {sources.map((source, sourceIndex) => (
                        <div className="response-rule-source" key={`${source.type}-${sourceIndex}`}>
                          <div className="response-rule-condition">
                            <span className="response-rule-source-priority" aria-label={t("优先级 {{number}}", { number: sourceIndex + 1 })}>{sourceIndex + 1}</span>
                            <Select aria-label={t("到期来源类型")} value={source.type} onChange={(event) => updateAt(index, (current) => ({ ...current, action: { ...current.action, expiry_sources: (current.action.expiry_sources ?? []).map((item, itemIndex) => itemIndex === sourceIndex ? normalizeResponseRuleExpirySourceType(event.target.value as ResponseRuleExpirySource["type"]) : item) } }))}>
                              {sourceTypes.map((type) => <option key={type} value={type}>{t(sourceTypeLabels[type])}</option>)}
                            </Select>
                            {source.type !== "retry_after" ? <Select aria-label={t("到期时间格式")} value={source.format ?? "delta_seconds"} onChange={(event) => updateAt(index, (current) => ({ ...current, action: { ...current.action, expiry_sources: (current.action.expiry_sources ?? []).map((item, itemIndex) => itemIndex === sourceIndex ? updateSource(item, "format", event.target.value) : item) } }))}>
                              {sourceFormats.map((format) => <option key={format} value={format}>{t(sourceFormatLabels[format])}</option>)}
                            </Select> : <span className="muted response-rule-source-help">{t("支持秒数和 HTTP-date")}</span>}
                            <Button variant="ghost" size="sm" onClick={() => moveExpirySource(index, sourceIndex, -1)} disabled={sourceIndex === 0} aria-label={t("上移到期来源")}> <ChevronUp size={14} /> </Button>
                            <Button variant="ghost" size="sm" onClick={() => moveExpirySource(index, sourceIndex, 1)} disabled={sourceIndex === sources.length - 1} aria-label={t("下移到期来源")}> <ChevronDown size={14} /> </Button>
                            <Button variant="ghost" size="sm" onClick={() => updateAt(index, (current) => ({ ...current, action: { ...current.action, expiry_sources: (current.action.expiry_sources ?? []).filter((_, itemIndex) => itemIndex !== sourceIndex) } }))} aria-label={t("删除到期来源")}><Trash2 size={14} /></Button>
                          </div>
                          {source.type === "header" ? <div><label className="field-label">{t("响应头名称")}</label><Input aria-label={t("响应头名称")} placeholder="X-Reset-At" value={source.header ?? ""} onChange={(event) => updateAt(index, (current) => ({ ...current, action: { ...current.action, expiry_sources: (current.action.expiry_sources ?? []).map((item, itemIndex) => itemIndex === sourceIndex ? updateSource(item, "header", event.target.value) : item) } }))} /><p className="muted">{t("填写一个合法的响应头字段名，例如 X-Reset-At。")}</p></div> : null}
                          {source.type === "json_pointer" ? <div><label className="field-label">{t("JSON Pointer")}</label><Input aria-label={t("JSON Pointer") } placeholder="/error/reset_at" value={source.json_pointer ?? ""} onChange={(event) => updateAt(index, (current) => ({ ...current, action: { ...current.action, expiry_sources: (current.action.expiry_sources ?? []).map((item, itemIndex) => itemIndex === sourceIndex ? updateSource(item, "json_pointer", event.target.value) : item) } }))} /><p className="muted">{t("例如 /error/reset_at，用于读取响应体 JSON 中的字段。")}</p></div> : null}
                      {source.type === "body_regex" ? <div><div className="response-rule-condition"><div><label className="field-label">{t("正文正则表达式")}</label><Input aria-label={t("正文正则表达式")} placeholder={'例如 "resets_at":"([^"]+)"'} value={source.regex ?? ""} onChange={(event) => updateAt(index, (current) => ({ ...current, action: { ...current.action, expiry_sources: (current.action.expiry_sources ?? []).map((item, itemIndex) => itemIndex === sourceIndex ? updateSource(item, "regex", event.target.value) : item) } }))} /></div><div><label className="field-label">{t("捕获组编号")}</label><Input aria-label={t("捕获组编号")} type="number" min={1} max={16} value={source.capture ?? 1} onChange={(event) => updateAt(index, (current) => ({ ...current, action: { ...current.action, expiry_sources: (current.action.expiry_sources ?? []).map((item, itemIndex) => itemIndex === sourceIndex ? updateSource(item, "capture", event.target.value) : item) } }))} /></div></div><p className="muted">{t("正则必须包含要解析的捕获组，捕获组编号从 1 开始。")}</p></div> : null}
                        </div>
                      ))}
                      </>}
                      <label className="field-label">{t("无法解析时的兜底")}</label>
                      <Select value={action.fallback ?? "none"} onChange={(event) => updateAt(index, (current) => ({ ...current, action: normalizeResponseRuleFallback(current.action, event.target.value as NonNullable<PlatformResponseRule["action"]["fallback"]>) }))}>
                        <option value="next_utc_midnight">{t("次日 UTC 00:00")}</option>
                        <option value="fixed_duration">{t("固定冷却时长")}</option>
                        <option value="none">{t("无法确定时不自动解除")}</option>
                      </Select>
                      {action.fallback === "fixed_duration" ? <div><label className="field-label">{t("固定冷却时长")}</label><Input aria-label={t("固定冷却时长")} placeholder={t("例如 1h") } value={action.fixed_duration ?? ""} onChange={(event) => updateAt(index, (current) => ({ ...current, action: { ...current.action, fixed_duration: event.target.value } }))} /><p className="muted">{t("例如 1h，表示从本次响应开始固定冷却 1 小时。")}</p></div> : null}
                    </>
                  ) : null}
                </div>
              </div>
            </article>
          );
        })}
      </div>
    </div>
  );
}
