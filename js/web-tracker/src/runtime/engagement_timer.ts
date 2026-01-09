/**
 * Engagement timer
 *
 * Counts time only while the page is:
 * - focused
 * - visible
 * - active (not pagehidden)
 *
 */
import type { WindowLike } from "../types.ts";

export function createEngagementTimer({
  windowRef,
  nowMs = () => Date.now(),
}: {
  windowRef?: WindowLike;
  nowMs?: () => number;
} = {}) {
  if (!windowRef) throw new Error("createEngagementTimer: windowRef is required");
  const w = windowRef;

  let started = false;
  let hasFocus = true;
  let isVisible = true;
  let isActive = true;

  let runningSinceMs: number | null = null;
  let accumulatedMs = 0;

  function readHasFocus() {
    try {
      if (typeof w.hasFocus === "function") return !!w.hasFocus();
      if (typeof w.document?.hasFocus === "function") return !!w.document.hasFocus();
    } catch {
      // ignore
    }
    return true;
  }

  function readIsVisible() {
    try {
      // In browsers: document.hidden is boolean.
      if (typeof w.document?.hidden === "boolean") return !w.document.hidden;
      if (typeof w.document?.visibilityState === "string")
        return w.document.visibilityState !== "hidden";
    } catch {
      // ignore
    }
    return true;
  }

  function shouldRun() {
    return hasFocus && isVisible && isActive;
  }

  function sync() {
    const now = nowMs();
    if (runningSinceMs != null) {
      const delta = Math.max(0, now - runningSinceMs);
      accumulatedMs += delta;
      runningSinceMs = now;
    }

    if (shouldRun()) {
      if (runningSinceMs == null) runningSinceMs = now;
    } else {
      runningSinceMs = null;
    }
  }

  function onFocus() {
    sync();
    hasFocus = true;
    sync();
  }

  function onBlur() {
    sync();
    hasFocus = false;
    sync();
  }

  function onVisibilityChange() {
    sync();
    isVisible = readIsVisible();
    sync();
  }

  function onPageShow() {
    sync();
    isActive = true;
    sync();
  }

  function onPageHide() {
    sync();
    isActive = false;
    sync();
  }

  function start() {
    if (started) return;
    started = true;

    hasFocus = readHasFocus();
    isVisible = readIsVisible();
    isActive = true;
    sync();

    if (typeof w.addEventListener === "function") {
      w.addEventListener("focus", onFocus);
      w.addEventListener("blur", onBlur);
      w.addEventListener("pageshow", onPageShow);
      w.addEventListener("pagehide", onPageHide);
    }
    if (typeof w.document?.addEventListener === "function") {
      w.document.addEventListener("visibilitychange", onVisibilityChange);
    }
  }

  function peek() {
    sync();
    return accumulatedMs | 0;
  }

  function getAndReset() {
    sync();
    const out = accumulatedMs | 0;
    accumulatedMs = 0;
    runningSinceMs = shouldRun() ? nowMs() : null;
    return out;
  }

  return { start, peek, getAndReset, __internal: { sync } };
}
