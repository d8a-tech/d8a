import type { WindowLike } from "../types.ts";

type WindowSlotContainer = WindowLike & Record<string, unknown>;

function windowDict(w: WindowLike): WindowSlotContainer {
  // WindowLike intentionally does NOT have an index signature because we want it to remain
  // assignable from the real `window` object. Keep the string-key indexing isolated here.
  return w as WindowSlotContainer;
}

export function getWindowSlot<T = unknown>(w: WindowLike, key: string): T | undefined {
  return windowDict(w)[key] as T | undefined;
}

export function setWindowSlot<T = unknown>(w: WindowLike, key: string, value: T) {
  windowDict(w)[key] = value;
}

export function ensureArraySlot<T = unknown>(w: WindowLike, key: string): T[] {
  const v = getWindowSlot<unknown>(w, key);
  if (Array.isArray(v)) return v;
  const arr: T[] = [];
  setWindowSlot<T[]>(w, key, arr);
  return arr;
}
