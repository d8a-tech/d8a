/**
 * Param resolution helpers with precedence rules:
 * - event params (per-event) override
 * - config params (per-property) fallback
 * - set params (global defaults) fallback
 *
 * `resolveString` returns a trimmed, non-empty string or null.
 * `resolveBool` returns a boolean or null.
 */
export function resolveWithPrecedence({
  key,
  eventParams,
  config,
  setParams,
}: {
  key: string;
  eventParams?: Record<string, unknown> | null;
  config?: Record<string, unknown> | null;
  setParams?: Record<string, unknown> | null;
}) {
  const ep = eventParams || {};
  const cfg = config || {};
  const sp = setParams || {};
  if (Object.prototype.hasOwnProperty.call(ep, key) && ep[key] != null) return ep[key];
  if (Object.prototype.hasOwnProperty.call(cfg, key) && cfg[key] != null) return cfg[key];
  if (Object.prototype.hasOwnProperty.call(sp, key) && sp[key] != null) return sp[key];
  return null;
}

export function resolveString({
  key,
  eventParams,
  config,
  setParams,
}: {
  key: string;
  eventParams?: Record<string, unknown> | null;
  config?: Record<string, unknown> | null;
  setParams?: Record<string, unknown> | null;
}) {
  const v = resolveWithPrecedence({ key, eventParams, config, setParams });
  return typeof v === "string" && v.trim() ? v.trim() : null;
}

export function resolveBool({
  key,
  eventParams,
  config,
  setParams,
}: {
  key: string;
  eventParams?: Record<string, unknown> | null;
  config?: Record<string, unknown> | null;
  setParams?: Record<string, unknown> | null;
}) {
  const v = resolveWithPrecedence({ key, eventParams, config, setParams });
  if (typeof v === "boolean") return v;
  return null;
}
