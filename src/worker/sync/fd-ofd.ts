import type {
  KernelWorkerFdEntrySnapshot,
  KernelWorkerOfdSlotSnapshot,
} from "../../messages.js";
import { snapshotKernelShareOfdSlots } from "../fork-shared.js";
import { ofdIdsFromFdEntries, snapshotProcessFdEntries } from "../ownership/fd-table.js";
import { collectProcessHeldOfds } from "../ownership/ofd-table.js";
import type { ProcessHandle } from "../types.js";

export function snapshotProcessFdOfdStateForKernelWorker(
  handle: ProcessHandle,
  vmPtr: number,
): {
  fdEntries: KernelWorkerFdEntrySnapshot[];
  ofdSlots: KernelWorkerOfdSlotSnapshot[];
} {
  const fdEntries = snapshotProcessFdEntries(handle, vmPtr);
  const ofdIds = [...new Set([
    ...ofdIdsFromFdEntries(handle, fdEntries),
    ...collectProcessHeldOfds(handle),
  ])];
  const ofdSlots = [...snapshotKernelShareOfdSlots(handle, ofdIds)]
    .map(([ofdId, slot]) => ({ ofdId, slot }));
  return { fdEntries, ofdSlots };
}
