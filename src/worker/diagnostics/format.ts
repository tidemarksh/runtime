const DEFAULT_DEBUG_KEYS = [
  "pc",
  "ra",
  "sp",
  "gp",
  "tp",
  "s11",
  "syscall",
  "process_thread_count",
  "blocked_addr",
  "futex_wait_addr",
  "futex_wait_expected",
  "futex_wait_timeout",
  "sig_deliver",
  "sig_pc",
  "sig_sp",
  "sig_s11",
  "zero_reg",
  "zero_reg_tid",
  "zero_reg_pc",
  "zero_reg_sp",
  "s11_write_tid",
  "s11_write_pc",
] as const;

export const WRITE_DEBUG_KEYS = [
  "pc",
  "ra",
  "sp",
  "gp",
  "tp",
  "s11",
  "syscall",
  "write_fd",
  "write_count",
  "write_result",
  "write_data",
] as const;

function extractDebugField(debug: string, key: string): string | null {
  const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = debug.match(new RegExp(`(?:^|\\s)${escapedKey}=("(?:\\\\.|[^"\\\\])*"|[^\\s]+)`));
  return match?.[1] ?? null;
}

export function readProcessPidField(debug?: string): number | null {
  if (!debug) {
    return null;
  }
  const value = extractDebugField(debug, "process_pid_field");
  if (value === null) {
    return null;
  }
  const pid = Number.parseInt(value, 10);
  return Number.isFinite(pid) ? pid : null;
}

export function formatCompactDebugSuffix(
  debug?: string,
  keys: readonly string[] = DEFAULT_DEBUG_KEYS,
): string {
  if (!debug) {
    return "";
  }
  const parts: string[] = [];
  for (const key of keys) {
    const value = extractDebugField(debug, key);
    if (value !== null) {
      parts.push(`${key}=${value}`);
    }
  }
  return parts.length > 0 ? `:${parts.join(":")}` : "";
}
