import type { ProcessHandle } from "../../types.js";

export function incrementBoundedCounter(
  counters: Record<string, number>,
  key: string,
  maxEntries = 32,
): void {
  const existing = counters[key];
  if (existing !== undefined) {
    counters[key] = existing + 1;
    return;
  }
  const entries = Object.keys(counters);
  if (entries.length < maxEntries) {
    counters[key] = 1;
    return;
  }
  for (const entry of entries) {
    const next = (counters[entry] ?? 0) - 1;
    if (next <= 0) {
      delete counters[entry];
      continue;
    }
    counters[entry] = next;
  }
}

export function hasSuspiciousLowStateDebug(debug: string | undefined): boolean {
  if (!debug) {
    return false;
  }
  return /(?:^|\s)pc=0x[0-9a-f]{1,3}(?:\s|$)/i.test(debug)
    || /(?:^|\s)sp=0x0(?:\s|$)/.test(debug)
    || /(?:^|\s)s11=0x0(?:\s|$)/.test(debug);
}

export function readLocalProcessPid(handle: ProcessHandle, vmPtr: number): number | null {
  if (vmPtr === 0) {
    return null;
  }
  try {
    const pid = handle.kernel.process_current_pid(vmPtr);
    return Number.isFinite(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}
