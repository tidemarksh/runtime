// Thread-worker tests in this file validate the Runtime sync-effects substrate.
import { expect, test, vi } from "vite-plus/test";
import {
  loadSyscallNumbers,
  readCompletedSyncEffects,
} from "../../../src/thread-worker/sync-effects.js";

const statusCodes = {
  OK: 0,
  FS: 6,
} as const;

test("loadSyscallNumbers keeps only thread-worker status snapshot syscalls", () => {
  const exports = {
    abi_sys_read: () => 63n,
    abi_sys_pipe2: () => 59n,
  };

  expect(loadSyscallNumbers(exports as never)).toEqual({
    read: 63n,
    pipe2: 59n,
  });
});

test("readCompletedSyncEffects delegates completed-effect classification to supervisor", () => {
  const takeCompletedSyncEffects = vi.fn(() => 4);
  const exports = {
    supervisor_thread_take_completed_sync_effects: takeCompletedSyncEffects,
  };

  expect(readCompletedSyncEffects(
    exports as never,
    statusCodes,
    statusCodes.OK,
    { read: 63n, pipe2: 59n },
    0x1234,
  )).toBe(4);
  expect(takeCompletedSyncEffects).toHaveBeenCalledWith(0x1234, statusCodes.OK);
});

test("readCompletedSyncEffects passes non-OK status to supervisor", () => {
  const takeCompletedSyncEffects = vi.fn(() => 0);
  const exports = {
    supervisor_thread_take_completed_sync_effects: takeCompletedSyncEffects,
  };

  expect(readCompletedSyncEffects(
    exports as never,
    statusCodes,
    statusCodes.FS,
    null,
    0x4321,
  )).toBe(0);
  expect(takeCompletedSyncEffects).toHaveBeenCalledWith(0x4321, statusCodes.FS);
});
