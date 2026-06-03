export type GuestMemoryPolicyLimitInput = number | bigint;

export const MIN_GUEST_MEMORY_POLICY_LIMIT_BYTES = 16n * 1024n * 1024n;

export function normalizeGuestMemoryPolicyLimitBytes(
  policyLimitBytes: GuestMemoryPolicyLimitInput,
): bigint {
  if (typeof policyLimitBytes === "bigint") {
    return policyLimitBytes < 0n ? 0n : policyLimitBytes;
  }
  if (!Number.isFinite(policyLimitBytes)) {
    return 0n;
  }
  if (Math.abs(policyLimitBytes) > Number.MAX_SAFE_INTEGER) {
    throw new RangeError("guestMemoryPolicyLimitBytes number exceeds JS safe integer range; pass a bigint");
  }
  return BigInt(Math.max(Math.trunc(policyLimitBytes), 0));
}

export function normalizeGuestMemoryPolicyLimitBytesWithMinimum(
  policyLimitBytes: GuestMemoryPolicyLimitInput | undefined,
  minimumBytes = MIN_GUEST_MEMORY_POLICY_LIMIT_BYTES,
): bigint {
  const normalized = policyLimitBytes === undefined
    ? minimumBytes
    : normalizeGuestMemoryPolicyLimitBytes(policyLimitBytes);
  return normalized < minimumBytes ? minimumBytes : normalized;
}
