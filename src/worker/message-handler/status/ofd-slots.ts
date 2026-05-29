import { applyThreadOfdSlots } from "../../ownership/ofd-slot.js";
import type { ProcessHandle } from "../../types.js";
import type { ThreadWorkerStatusMessage } from "../types.js";
import { appendStatusSocketDebug } from "./debug.js";

export function applyStatusOfdSlots(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
): void {
  applyThreadOfdSlots(handle, msg, appendStatusSocketDebug);
}
