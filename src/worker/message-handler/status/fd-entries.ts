import { applyThreadFdEntries } from "../../ownership/fd-table.js";
import type { ProcessHandle } from "../../types.js";
import type { ThreadWorkerStatusMessage } from "../types.js";
import { appendStatusSocketDebug } from "./debug.js";

export function applyStatusFdEntries(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
): void {
  applyThreadFdEntries(handle, msg.vmPtr, msg.fdEntries, appendStatusSocketDebug);
}
