import {
  applyThreadEventfdTraceOffset,
  applyThreadOfdSlots,
} from "../../ownership/ofd-slot.js";
import type { ProcessHandle } from "../../types.js";
import type { ThreadWorkerStatusMessage } from "../types.js";
import { appendStatusSocketDebug } from "./debug.js";

export function applyStatusEventfdTraceOffset(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
): boolean {
  return applyThreadEventfdTraceOffset(handle, msg);
}

export function applyStatusOfdSlots(
  handle: ProcessHandle,
  msg: ThreadWorkerStatusMessage,
  options: { trustRegularFileOfdOffsets?: boolean } = {},
): void {
  applyThreadOfdSlots(
    handle,
    {
      ...msg,
      trustRegularFileOfdOffsets: options.trustRegularFileOfdOffsets,
    },
    appendStatusSocketDebug,
  );
}
