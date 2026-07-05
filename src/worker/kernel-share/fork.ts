import type { ProcessHandle } from "../types.js";
import {
  addKernelShareMember,
  deleteKernelShareMember,
  processKernelShareGroup,
  setProcessKernelShareGroup,
} from "./common.js";

export function adoptKernelShare(parent: ProcessHandle, child: ProcessHandle): void {
  setProcessKernelShareGroup(child, processKernelShareGroup(parent));
  addKernelShareMember(child, child);
}

export function detachKernelShare(handle: ProcessHandle): void {
  deleteKernelShareMember(handle, handle);
}
