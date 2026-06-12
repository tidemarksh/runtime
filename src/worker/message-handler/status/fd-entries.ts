import {
  applyThreadFdEntries,
  canonicalFdTableVmPtrForThread,
} from "../../ownership/fd-table.js";
import type { ProcessHandle } from "../../types.js";
import type { ThreadWorkerStatusMessage } from "../types.js";
import { appendStatusSocketDebug } from "./debug.js";

export function applyStatusFdEntries(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
): void {
  const options = {
    appendStatusSocketDebug,
    preserveLiveRegularFileDescriptors: true,
    syscallNr: msg.syscallNr,
  };
  applyThreadFdEntries(handle, msg.vmPtr, msg.fdEntries, options);
  const canonicalVmPtr = canonicalFdTableVmPtrForThread(handle, msg.vmPtr);
  if (canonicalVmPtr !== msg.vmPtr) {
    applyThreadFdEntries(handle, canonicalVmPtr, msg.fdEntries, options);
  }
}
