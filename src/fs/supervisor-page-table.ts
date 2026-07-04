import type { KernelExports } from "../abi.js";
import type {
  ResidentPageEvictStatus,
  ResidentPageTableAuthority,
} from "./sab-page-cache.js";

function supervisorNumber(value: bigint | number | undefined, name: string): number {
  const numeric = typeof value === "bigint" ? Number(value) : value;
  if (typeof numeric !== "number" || !Number.isSafeInteger(numeric)) {
    throw new Error(`invalid ${name}: ${String(value)}`);
  }
  return numeric;
}

function supervisorNumericExport(
  kernel: KernelExports,
  name:
    | "supervisor_fs_resident_page_evict_not_found"
    | "supervisor_fs_resident_page_evict_dirty_pinned"
    | "supervisor_fs_resident_page_evict_evicted",
  label: string,
): number {
  const read = kernel[name];
  if (typeof read !== "function") {
    throw new Error(`missing ${name} export`);
  }
  return supervisorNumber(read.call(kernel), label);
}

export function supervisorResidentPageTableAuthority(kernel: KernelExports): ResidentPageTableAuthority {
  const slotDecision = kernel.supervisor_fs_resident_page_slot_decision;
  const evictStatus = kernel.supervisor_fs_resident_page_evict_status;
  if (typeof slotDecision !== "function") {
    throw new Error("missing supervisor_fs_resident_page_slot_decision export");
  }
  if (typeof evictStatus !== "function") {
    throw new Error("missing supervisor_fs_resident_page_evict_status export");
  }
  const notFoundStatus = supervisorNumericExport(
    kernel,
    "supervisor_fs_resident_page_evict_not_found",
    "resident page evict not-found status",
  );
  const dirtyPinnedStatus = supervisorNumericExport(
    kernel,
    "supervisor_fs_resident_page_evict_dirty_pinned",
    "resident page evict dirty-pinned status",
  );
  const evictedStatus = supervisorNumericExport(
    kernel,
    "supervisor_fs_resident_page_evict_evicted",
    "resident page evict evicted status",
  );

  return {
    selectPublishSlot(input) {
      return supervisorNumber(
        slotDecision.call(kernel, input.existingSlot, input.emptySlot, input.cleanEvictSlot),
        "supervisor resident page slot decision",
      );
    },
    decideEviction(input): ResidentPageEvictStatus {
      const status = supervisorNumber(
        evictStatus.call(kernel, input.recordFound ? 1 : 0, input.dirty ? 1 : 0),
        "supervisor resident page evict status",
      );
      if (status === notFoundStatus) {
        return "not-found";
      }
      if (status === dirtyPinnedStatus) {
        return "dirty-pinned";
      }
      if (status === evictedStatus) {
        return "evicted";
      }
      throw new Error(`unknown supervisor resident page evict status: ${status}`);
    },
  };
}
