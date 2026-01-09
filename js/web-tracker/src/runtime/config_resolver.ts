import type { PropertyConfig, RuntimeState } from "../types.ts";

export function getPropertyIds(getState: () => RuntimeState) {
  const s = typeof getState === "function" ? getState() : null;
  if (Array.isArray(s?.propertyIds) && s.propertyIds.length > 0) return s.propertyIds.slice();
  return [];
}

export function getPropertyConfig(
  getState: () => RuntimeState,
  propertyId: string,
): PropertyConfig {
  const s = typeof getState === "function" ? getState() : null;
  const cfg = s?.propertyConfigs?.[propertyId];
  return cfg && typeof cfg === "object" ? cfg : {};
}

export function getSetParams(getState: () => RuntimeState): Record<string, unknown> {
  const s = typeof getState === "function" ? getState() : null;
  return s?.set && typeof s.set === "object" ? s.set : {};
}
