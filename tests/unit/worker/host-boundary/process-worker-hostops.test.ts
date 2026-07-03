import { expect, test, vi } from "vite-plus/test";
import type { KernelExports } from "../../../../src/abi.js";
import {
  applySupervisorProcessMemoryPlan,
  readSupervisorProcessMemoryPlan,
  supervisorProcessMemoryInitialBytes,
  supervisorProcessMemoryMaxPages,
} from "../../../../src/worker/supervisor-host-op.js";
import { makeSupervisorAbiStubs } from "../support/supervisor-abi.js";
import {
  makeHostBoundaryHandle,
  makeHostBoundaryKernel,
} from "./helpers.js";

test("process host boundary applies supervisor CreateProcessMemory and CopyMemoryRanges records", () => {
  const source = new WebAssembly.Memory({ initial: 2, maximum: 8, shared: true });
  const target = new WebAssembly.Memory({ initial: 1, maximum: 8, shared: true });
  const kernel = makeHostBoundaryKernel();
  const worker = { postMessage: vi.fn() } as unknown as Worker;

  kernel.memory = source;
  Object.assign(kernel, makeSupervisorAbiStubs({
    memory: source,
    currentExports: () => kernel as KernelExports,
    abi_spawn_kind_execve: () => 9,
    thread_spawn_kind: () => 1,
    thread_spawn_address_profile: () => 1,
    process_guest_base_for_address_profile: () => 0x1000,
    thread_process_ptr: () => 0x2000,
    process_state_bytes: () => 0x1000,
    thread_sp: () => 0n,
    thread_spawn_child_pid: () => 42,
    thread_spawn_process_max_memory_pages: () => 64,
  }));
  const handle = makeHostBoundaryHandle(kernel, worker);
  handle.memory = source;

  const sourceBytes = new Uint8Array(source.buffer);
  sourceBytes[0x1000] = 0xaa;
  sourceBytes[0x2fff] = 0xbb;
  sourceBytes[0x3fff] = 0xcc;

  const plan = readSupervisorProcessMemoryPlan(handle, handle.vmPtr);

  expect(plan.create.kind).toBe(kernel.supervisor_host_op_kind_create_process_memory?.());
  expect(plan.create.tid).toBe(42);
  expect(plan.copyRanges).toEqual([{ start: 0x1000, end: 0x4000 }]);
  expect(supervisorProcessMemoryInitialBytes(plan)).toBeGreaterThanOrEqual(0x4000);
  expect(supervisorProcessMemoryMaxPages(plan)).toBe(64);

  applySupervisorProcessMemoryPlan(source, target, plan);

  const targetBytes = new Uint8Array(target.buffer);
  expect(targetBytes[0x1000]).toBe(0xaa);
  expect(targetBytes[0x2fff]).toBe(0xbb);
  expect(targetBytes[0x3fff]).toBe(0xcc);
});
