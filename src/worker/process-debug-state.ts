import type { ProcessHandle } from "./types.js";

const processDebugCommands = new WeakMap<ProcessHandle, string>();
const processDebugMessages = new WeakMap<ProcessHandle, string>();

type LegacyProcessDebugSeed = {
  debugCommand?: string;
  lastDebugMessage?: string | null;
};

export function processDebugCommand(handle: ProcessHandle): string {
  const command = processDebugCommands.get(handle);
  if (command !== undefined) {
    return command;
  }
  const legacyCommand = (handle as LegacyProcessDebugSeed).debugCommand;
  if (legacyCommand !== undefined) {
    processDebugCommands.set(handle, legacyCommand);
    delete (handle as LegacyProcessDebugSeed).debugCommand;
    return legacyCommand;
  }
  return "<entry>";
}

export function setProcessDebugCommand(handle: ProcessHandle, command: string): void {
  delete (handle as LegacyProcessDebugSeed).debugCommand;
  processDebugCommands.set(handle, command);
}

export function processLastDebugMessage(handle: ProcessHandle): string | undefined {
  const message = processDebugMessages.get(handle);
  if (message !== undefined) {
    return message;
  }
  const legacyMessage = (handle as LegacyProcessDebugSeed).lastDebugMessage;
  if (legacyMessage !== undefined) {
    delete (handle as LegacyProcessDebugSeed).lastDebugMessage;
    if (legacyMessage !== null) {
      processDebugMessages.set(handle, legacyMessage);
      return legacyMessage;
    }
  }
  return undefined;
}

export function setProcessLastDebugMessage(
  handle: ProcessHandle,
  message: string | null | undefined,
): void {
  delete (handle as LegacyProcessDebugSeed).lastDebugMessage;
  if (message === undefined || message === null) {
    processDebugMessages.delete(handle);
    return;
  }
  processDebugMessages.set(handle, message);
}
