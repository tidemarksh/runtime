import { expect, test } from "vite-plus/test";
import type { KernelExports } from "../../../src/abi.js";
import {
  encodeSupervisorShadowThreadEvent,
  hasSupervisorShadowProtocol,
  submitSupervisorShadowThreadEvent,
  SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC,
  SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES,
  SUPERVISOR_SHADOW_THREAD_EVENT_VERSION,
  type SupervisorShadowThreadEvent,
} from "../../../src/worker/supervisor-shadow.js";

function makeEvent(): SupervisorShadowThreadEvent {
  return {
    sequence: 3n,
    vmPtr: 0x2000n,
    pid: 7,
    tid: 11,
    status: 20,
    syncEffects: 4,
    blockedReason: 2,
    pc: 0x1234n,
    sp: 0x5678n,
  };
}

test("hasSupervisorShadowProtocol treats supervisor exports as optional", () => {
  const kernelExports = {
    memory: new WebAssembly.Memory({ initial: 1 }),
  } as unknown as KernelExports;

  expect(hasSupervisorShadowProtocol(kernelExports)).toBe(false);
});

test("encodeSupervisorShadowThreadEvent writes a deterministic little-endian record", () => {
  const first = new Uint8Array(SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES);
  const second = new Uint8Array(SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES);
  encodeSupervisorShadowThreadEvent(new DataView(first.buffer), 0, makeEvent());
  encodeSupervisorShadowThreadEvent(new DataView(second.buffer), 0, makeEvent());

  expect([...first]).toEqual([...second]);

  const view = new DataView(first.buffer);
  expect(view.getUint32(0, true)).toBe(SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC);
  expect(view.getUint32(4, true)).toBe(SUPERVISOR_SHADOW_THREAD_EVENT_VERSION);
  expect(view.getUint32(8, true)).toBe(SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES);
  expect(view.getUint32(12, true)).toBe(20);
  expect(view.getBigUint64(16, true)).toBe(3n);
  expect(view.getBigUint64(24, true)).toBe(0x2000n);
  expect(view.getUint32(32, true)).toBe(7);
  expect(view.getUint32(36, true)).toBe(11);
  expect(view.getUint32(40, true)).toBe(4);
  expect(view.getUint32(44, true)).toBe(2);
  expect(view.getBigUint64(48, true)).toBe(0x1234n);
  expect(view.getBigUint64(56, true)).toBe(0x5678n);
});

test("submitSupervisorShadowThreadEvent writes the record and calls the kernel export", () => {
  const memory = new WebAssembly.Memory({ initial: 1 });
  const submissions: Array<[number, number]> = [];
  const kernelExports = {
    memory,
    supervisor_abi_version: () => 1,
    supervisor_submit_thread_event: (ptr: number, len: number) => {
      submissions.push([ptr, len]);
      return 0;
    },
  } as unknown as KernelExports;

  const result = submitSupervisorShadowThreadEvent(kernelExports, 32, makeEvent());

  expect(result).toBe(0);
  expect(submissions).toEqual([[32, SUPERVISOR_SHADOW_THREAD_EVENT_RECORD_BYTES]]);
  const view = new DataView(memory.buffer);
  expect(view.getUint32(32, true)).toBe(SUPERVISOR_SHADOW_THREAD_EVENT_MAGIC);
  expect(view.getUint32(44, true)).toBe(20);
});
