export function createDebugLogger({ enabled }: { enabled?: boolean }) {
  return {
    enabled: enabled === true,
    log: (...args: unknown[]) => {
      if (enabled !== true) return;
      try {
        console.log(...args);
      } catch {
        // ignore
      }
    },
  };
}
