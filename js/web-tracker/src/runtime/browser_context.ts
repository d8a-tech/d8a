import type { BrowserContext, WindowLike } from "../types.ts";

export function getBrowserContext(w: WindowLike): BrowserContext {
  const dl = (() => {
    try {
      return String(w?.location?.href || "");
    } catch {
      return "";
    }
  })();

  const dt = (() => {
    try {
      return String(w?.document?.title || "");
    } catch {
      return "";
    }
  })();

  const dr = (() => {
    try {
      return String(w?.document?.referrer || "");
    } catch {
      return "";
    }
  })();

  const dh = (() => {
    try {
      return String(w?.location?.hostname || "");
    } catch {
      return "";
    }
  })();

  const ul = (() => {
    try {
      return String(w?.navigator?.language || "");
    } catch {
      return "";
    }
  })();

  const sr = (() => {
    try {
      const sw = w?.screen?.width;
      const sh = w?.screen?.height;
      if (typeof sw === "number" && typeof sh === "number") return `${sw}x${sh}`;
    } catch {
      // ignore
    }
    return "";
  })();

  return { dl, dt, dr, dh, ul, sr };
}
