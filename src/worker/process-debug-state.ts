import type { ProcessHandle } from "./types.js";

const processDebugCommands = new WeakMap<ProcessHandle, string>();
const processDebugMessages = new WeakMap<ProcessHandle, string>();

export function processDebugCommand(handle: ProcessHandle): string {
  const command = processDebugCommands.get(handle);
  if (command !== undefined) {
    return command;
  }
  return "<entry>";
}

export function setProcessDebugCommand(handle: ProcessHandle, command: string): void {
  processDebugCommands.set(handle, command);
}

export function processLastDebugMessage(handle: ProcessHandle): string | undefined {
  const message = processDebugMessages.get(handle);
  if (message !== undefined) {
    return message;
  }
  return undefined;
}

export function setProcessLastDebugMessage(
  handle: ProcessHandle,
  message: string | null | undefined,
): void {
  if (message === undefined || message === null) {
    processDebugMessages.delete(handle);
    return;
  }
  processDebugMessages.set(handle, message);
}
