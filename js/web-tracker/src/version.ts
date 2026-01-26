/**
 * Package version identifier.
 * This value is replaced at build time via esbuild define.
 */
declare const __D8A_VERSION__: string;

function devVersionUtc(): string {
  const now = new Date();
  const year = String(now.getUTCFullYear()).slice(-2);
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  return `dev-${year}-${month}`;
}

export const version: string =
  typeof __D8A_VERSION__ !== "undefined" && __D8A_VERSION__ ? __D8A_VERSION__ : devVersionUtc();
