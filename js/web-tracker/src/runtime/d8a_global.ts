/**
 * Creates a `d8a(...)` function compatible with the snippet API.
 *
 * Required behavior:
 * - `d8a('event', ...)` behaves the same as `d8a.event(...)` etc.
 * - The function form pushes to `dataLayer`.
 */
import { ensureArraySlot } from "../utils/window_slots.ts";

export function createD8aGlobal({
  windowRef,
  dataLayerName = "d8aLayer",
}: {
  windowRef: import("../types.ts").WindowLike;
  dataLayerName?: string;
}) {
  const w = windowRef;
  if (!w) throw new Error("createD8aGlobal: windowRef is required");

  function d8a(..._args: unknown[]) {
    // We intentionally ignore `_args` and push the raw `arguments` object (snippet compatibility).
    void _args;
    // Match the snippet: push the raw arguments so a loader can interpret them.
    const dl = dataLayerName;
    const arr = ensureArraySlot<unknown>(w, dl);
    arr.push(arguments);
  }

  // Method aliases (same behavior as commands).
  d8a.js = (date: unknown) => d8a("js", date);
  d8a.config = (propertyId: unknown, params: unknown) => d8a("config", propertyId, params);
  d8a.event = (eventName: unknown, params: unknown) => d8a("event", eventName, params);
  d8a.set = (a: unknown, b?: unknown) => (b === undefined ? d8a("set", a) : d8a("set", a, b));
  d8a.consent = (action: unknown, state: unknown) => d8a("consent", action, state);

  return d8a;
}
