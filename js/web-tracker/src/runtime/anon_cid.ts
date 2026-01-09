import { generateCidParts, formatCid } from "../cookies/d8a_cookies.ts";

/**
 * Generates a cookieless cid using the same logic as our normal `_d8a` cookie
 * value generator, but without persisting it anywhere.
 *
 * This keeps the cid format consistent across consented and non-consented flows.
 */
export function generateAnonCid() {
  const parts = generateCidParts({ nowMs: Date.now() });
  return formatCid(parts);
}

export function getOrCreateAnonCid({
  windowRef,
  state,
}: {
  windowRef?: unknown;
  state?: { anonCid?: string | null } | null;
}) {
  // windowRef kept for future use (e.g., crypto-based generator), but not needed today.
  void windowRef;
  if (!state) return generateAnonCid();
  if (typeof state.anonCid === "string" && state.anonCid) return state.anonCid;
  state.anonCid = generateAnonCid();
  return state.anonCid;
}
