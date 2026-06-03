/**
 * Kernel WASM integration test.
 *
 * Loads the kernel .wasm, instantiates it directly (no Workers),
 * runs a minimal RISC-V ELF, and verifies exit code = 0.
 */

import { expect, test } from "vite-plus/test";
import { readFileSync } from "node:fs";
import {
  attachKernelRuntimeState,
  kernelOfdStateBytes,
  kernelOfdStatePtr,
  kernelSocketStateBytes,
  kernelSocketStatePtr,
  readKernelRuntimeState,
  readStatusCodes,
  type KernelExports,
} from "../../../src/abi.js";
import { createKernelEnv } from "../../../src/wasm/env.js";
import { instantiateKernelWithSharedMemory } from "../../../src/wasm/kernel-memory.js";
import { stageExecveGuestHandoff } from "../../../src/worker/fork/execve-handoff.js";
import {
  copyKernelPrivateRegion,
  ensureMemoryCapacity,
  writeKernelScratch,
} from "../../../src/worker/kernel-scratch.js";
import {
  FS_RING_HEADER_BYTES,
  FS_RING_REQUEST_BUF_PTR_OFFSET,
  FS_RING_REQUEST_LEN_OFFSET,
  FS_RING_REQUEST_PATH_PTR_OFFSET,
  FS_RING_REQUEST_VM_PTR_OFFSET,
  FS_RING_SUBMIT_HEAD_OFFSET,
  FS_RING_SUBMIT_TAIL_OFFSET,
} from "../../../src/layout/fs-ring.js";
import { findRiscvElf, logSkip, readKernelWasm } from "../../support/runtime-workload.js";

const TARGET_KERNEL_WASM_PATH = process.env.TIDEMARK_TEST_TARGET_KERNEL_WASM ?? "";

function encodeI(opcode: number, rd: number, funct3: number, rs1: number, imm: number): number {
  return (((imm & 0xfff) << 20) | (rs1 << 15) | (funct3 << 12) | (rd << 7) | opcode) >>> 0;
}

function encodeB(funct3: number, rs1: number, rs2: number, imm: number): number {
  return (
    (((imm >> 12) & 0x1) << 31)
    | (((imm >> 5) & 0x3f) << 25)
    | (rs2 << 20)
    | (rs1 << 15)
    | (funct3 << 12)
    | (((imm >> 1) & 0xf) << 8)
    | (((imm >> 11) & 0x1) << 7)
    | 0x63
  ) >>> 0;
}

function writeU16(memory: WebAssembly.Memory, addr: number, value: number): void {
  new DataView(memory.buffer).setUint16(addr, value, true);
}

function writeU32(memory: WebAssembly.Memory, addr: number, value: number): void {
  new DataView(memory.buffer).setUint32(addr, value >>> 0, true);
}

function writeU64(memory: WebAssembly.Memory, addr: number, value: bigint): void {
  new DataView(memory.buffer).setBigUint64(addr, value, true);
}

function readU64(memory: WebAssembly.Memory, addr: number): bigint {
  return new DataView(memory.buffer).getBigUint64(addr, true);
}

function threadRegOffset(regIndex: number): number {
  return regIndex * 8;
}

const THREAD_PC_OFFSET = 32 * 8;
const THREAD_FAULT_PC_OFFSET = 688;
const THREAD_FAULT_ADDR_OFFSET = 696;

function readThreadReg(
  exports: KernelExports,
  memory: WebAssembly.Memory,
  vmPtr: number,
  regIndex: number,
): bigint {
  return exports.thread_reg?.(vmPtr, regIndex) ?? readU64(memory, vmPtr + threadRegOffset(regIndex));
}

function readThreadRa(exports: KernelExports, memory: WebAssembly.Memory, vmPtr: number): bigint {
  return exports.thread_ra?.(vmPtr) ?? readThreadReg(exports, memory, vmPtr, 1);
}

function readThreadFaultPc(exports: KernelExports, memory: WebAssembly.Memory, vmPtr: number): bigint {
  return exports.thread_fault_pc?.(vmPtr) ?? readU64(memory, vmPtr + THREAD_FAULT_PC_OFFSET);
}

function readThreadFaultAddr(exports: KernelExports, memory: WebAssembly.Memory, vmPtr: number): bigint {
  return exports.thread_fault_addr?.(vmPtr) ?? readU64(memory, vmPtr + THREAD_FAULT_ADDR_OFFSET);
}

const SYS_CLONE = 220;
const SYS_FUTEX = 98;
const SYS_MMAP = 222;
const SYS_MPROTECT = 226;
const SYS_RISCV_FLUSH_ICACHE = 259;
const SIGSEGV = 11;
const FS_OP_OPENAT = 3;
const CLONE_VM = 0x00000100;
const CLONE_FS = 0x00000200;
const CLONE_SIGHAND = 0x00000800;
const CLONE_THREAD = 0x00010000;
const AT_FDCWD = -100;
const O_RDONLY = 0;
const O_CREAT = 0x40;
const PROT_NONE = 0;
const PROT_READ = 1;
const PROT_WRITE = 2;
const PROT_EXEC = 4;

function targetAddressProfile(exports: KernelExports): number {
  const profile = exports.abi_guest_address_profile_target?.();
  if (profile === undefined || profile === 0) {
    throw new Error("kernel ABI missing target guest address profile");
  }
  return profile;
}

function createProcessWithPolicy(
  exports: KernelExports,
  elfOffset: number,
  elfLength: number,
  guestMemoryPolicyLimitBytes: number,
  stackSize: number,
): number {
  const create = exports.process_create_with_memory_policy;
  if (!create) {
    throw new Error("kernel ABI missing process_create_with_memory_policy");
  }
  return create(
    elfOffset,
    elfLength,
    targetAddressProfile(exports),
    BigInt(guestMemoryPolicyLimitBytes),
    0,
    stackSize,
  );
}

function setExecveMemoryHandoff(
  exports: KernelExports,
  vmPtr: number,
  processPtr: number,
  pathPtr: bigint,
  argvPtr: bigint,
  envpPtr: bigint,
  guestMemoryPolicyLimitBytes: number,
): void {
  const setHandoff = exports.thread_set_execve_memory_handoff;
  if (!setHandoff) {
    throw new Error("kernel ABI missing thread_set_execve_memory_handoff");
  }
  setHandoff(
    vmPtr,
    processPtr,
    pathPtr,
    argvPtr,
    envpPtr,
    targetAddressProfile(exports),
    BigInt(guestMemoryPolicyLimitBytes),
    0,
  );
}
const MAP_PRIVATE = 0x02;
const MAP_FIXED = 0x10;
const MAP_ANONYMOUS = 0x20;
const encoder = new TextEncoder();

function cstr(value: string): Uint8Array {
  return encoder.encode(`${value}\0`);
}

function copyKernelRuntimeRegion(
  source: WebAssembly.Memory,
  target: WebAssembly.Memory,
  start: number,
  byteLength: number,
): void {
  if (start === 0 || byteLength <= 0) {
    return;
  }
  const end = start + byteLength;
  ensureMemoryCapacity(target, end);
  new Uint8Array(target.buffer).set(new Uint8Array(source.buffer, start, byteLength), start);
}

function copyExecveKernelRuntimeRegions(
  source: KernelExports,
  sourceMemory: WebAssembly.Memory,
  targetMemory: WebAssembly.Memory,
): void {
  const state = readKernelRuntimeState(source);
  copyKernelRuntimeRegion(sourceMemory, targetMemory, kernelOfdStatePtr(state), kernelOfdStateBytes(source, state));
  copyKernelRuntimeRegion(
    sourceMemory,
    targetMemory,
    state.memfsPtr,
    state.memfsHeaderBytes
      + state.memfsInodesBytes
      + state.memfsFirstChildBytes
      + state.memfsDirEntriesBytes
      + state.memfsDataBytes,
  );
  copyKernelRuntimeRegion(sourceMemory, targetMemory, state.epollTablePtr, source.kernel_epoll_table_bytes());
  copyKernelRuntimeRegion(sourceMemory, targetMemory, kernelSocketStatePtr(state), kernelSocketStateBytes(source, state));
  copyKernelRuntimeRegion(sourceMemory, targetMemory, state.pipeBase, source.kernel_pipe_region_bytes());
  copyKernelRuntimeRegion(sourceMemory, targetMemory, state.netBufBase, source.kernel_net_buf_pool_bytes());
  copyKernelRuntimeRegion(
    sourceMemory,
    targetMemory,
    state.sigSlotsPtr,
    source.kernel_sig_slots_bytes() + source.kernel_signal_action_table_bytes(),
  );
  copyKernelRuntimeRegion(
    sourceMemory,
    targetMemory,
    state.processPendingPtr,
    source.kernel_process_pending_bytes(),
  );
  copyKernelRuntimeRegion(
    sourceMemory,
    targetMemory,
    state.processRuntimeStatePtr,
    source.kernel_process_runtime_state_bytes(),
  );
  copyKernelRuntimeRegion(sourceMemory, targetMemory, state.timeStatePtr, source.kernel_time_state_bytes());
}

function installExecutableFile(exports: KernelExports, name: string, data: Uint8Array): void {
  const nameBytes = encoder.encode(name);
  const namePtr = writeKernelScratch(exports, nameBytes);
  const dataPtr = writeKernelScratch(exports, data, 0x1000);
  const ino = exports.memfs_create_file_with_data(1, namePtr, nameBytes.length, dataPtr, data.length);
  expect(ino).toBeGreaterThan(0);
  expect(exports.memfs_set_mode(ino, 0o755)).toBe(0n);
}

async function loadKernel(opts?: { memory?: WebAssembly.Memory }) {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return null;
  }
  const module = await WebAssembly.compile(wasmBytes);
  // Kernel WASM imports env.memory. Kernel data at 48MB (--global-base),
  // so memory must be at least 48MB + kernel data (~4KB) = ~49MB.
  // Use 64MB (1024 pages) as a safe default.
  const memory = opts?.memory ?? new WebAssembly.Memory({
    initial: 1024,   // 64 MB
    maximum: 32768,  // 2 GB
    shared: true,
  });
  const instance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(memory),
  });
  const exports = instance.exports as unknown as KernelExports;
  return { exports, memory, module };
}

async function instantiateKernelArtifact(opts: { wasmBytes: Uint8Array; memory: WebAssembly.Memory }) {
  const module = await WebAssembly.compile(opts.wasmBytes);
  const instance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(opts.memory),
  });
  return { exports: instance.exports as unknown as KernelExports, memory: opts.memory, module };
}

async function loadKernelTargetArtifactWithSharedI64Memory() {
  if (!TARGET_KERNEL_WASM_PATH) {
    return logSkip("missing target kernel wasm path: set TIDEMARK_TEST_TARGET_KERNEL_WASM");
  }
  const memory = new WebAssembly.Memory({
    initial: 771n,
    maximum: 65536n,
    shared: true,
    address: "i64",
  } as unknown as WebAssembly.MemoryDescriptor);
  return instantiateKernelArtifact({
    wasmBytes: new Uint8Array(readFileSync(TARGET_KERNEL_WASM_PATH)),
    memory,
  });
}

test("kernel WASM loads and exports constants", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports } = loaded;
  const status = readStatusCodes(exports);

  expect(status.OK).toBe(0);
  expect(status.FAULT).toBe(3);
  expect(exports.abi_page_size()).toBe(4096);

  // kernel_init should not crash. heapBase must be >= __heap_base (kernel data end).
  const heapBase0 = (exports as any).__heap_base?.value ?? 0x200000;
  exports.kernel_init(heapBase0);
});

test("target kernel artifact instantiates with shared i64 memory", async () => {
  const loaded = await loadKernelTargetArtifactWithSharedI64Memory();
  if (!loaded) {
    return;
  }
  const { exports, memory } = loaded;
  expect(memory.buffer).toBeInstanceOf(SharedArrayBuffer);
  expect(exports.memory).toBe(memory);
  expect(exports.abi_guest_address_profile_target?.()).toBeGreaterThan(0);

  const heapBase = (exports as any).__heap_base?.value ?? 0x200000;
  exports.kernel_init(Number(heapBase));
  expect(exports.abi_page_size()).toBe(4096);
});

test("kernel shared memory helper instantiates the current kernel artifact", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const module = await WebAssembly.compile(wasmBytes);
  const { instance, memory } = await instantiateKernelWithSharedMemory(module, {
    initialPages: 1024,
    maximumPages: 65536,
  });
  const exports = instance.exports as unknown as KernelExports;
  expect(exports.memory).toBe(memory);
  const heapBase = (exports as any).__heap_base?.value ?? 0x200000;
  exports.kernel_init(Number(heapBase));
  expect(exports.abi_page_size()).toBe(4096);
});

test("kernel shared memory helper instantiates the target kernel artifact", async () => {
  if (!TARGET_KERNEL_WASM_PATH) {
    return logSkip("missing target kernel wasm path: set TIDEMARK_TEST_TARGET_KERNEL_WASM");
  }
  const module = await WebAssembly.compile(new Uint8Array(readFileSync(TARGET_KERNEL_WASM_PATH)));
  const { instance, memory } = await instantiateKernelWithSharedMemory(module, {
    initialPages: 771,
    maximumPages: 65536,
  });
  const exports = instance.exports as unknown as KernelExports;
  expect(exports.memory).toBe(memory);
  expect(exports.abi_guest_address_profile_target?.()).toBeGreaterThan(0);
  const heapBase = (exports as any).__heap_base?.value ?? 0x200000;
  exports.kernel_init(Number(heapBase));
  expect(exports.abi_page_size()).toBe(4096);
});

test("kernel_init + process_create + thread_step runs to exit", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports, memory } = loaded;

  const elfData = findRiscvElf("add");
  if (!elfData) {
    logSkip("add.elf not found");
    return;
  }

  // heapBase must be >= __heap_base (kernel data segments end at ~48MB)
  const heapBase1 = (exports as any).__heap_base?.value ?? 0x200000;
  exports.kernel_init(heapBase1);

  // Copy ELF to WASM memory (re-read buffer after potential grow)
  const ramSize = memory.buffer.byteLength;
  const elfOffset = ramSize - 256 * 1024;
  const memView = new Uint8Array(memory.buffer);
  memView.set(elfData, elfOffset);

  const vmPtr = createProcessWithPolicy(exports, elfOffset, elfData.length, ramSize, 1024 * 1024);
  expect(vmPtr).toBeGreaterThan(0);

  const STATUS_OK = exports.abi_status_ok();
  const STATUS_FAULT = exports.abi_status_fault();
  let totalSteps = 0;

  while (totalSteps < 50_000_000) {
    const remaining = exports.thread_step(vmPtr, 500_000);
    totalSteps += 500_000 - remaining;
    const s = exports.thread_status(vmPtr);

    if (s === STATUS_FAULT) {
      expect(exports.thread_exit_code(vmPtr)).toBe(0);
      return;
    }
    if (s !== STATUS_OK) break;
  }

  throw new Error(`Did not exit within ${totalSteps} instructions`);
});

test("detached kernel instance completes staged execve handoff", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports: owner, memory, module } = loaded;

  const elfData = findRiscvElf("add");
  if (!elfData) {
    logSkip("add.elf not found");
    return;
  }

  const heapBase = (owner as any).__heap_base?.value ?? 0x200000;
  owner.kernel_init(heapBase);
  installExecutableFile(owner, "child", elfData);

  const ramSize = memory.buffer.byteLength;
  const elfOffset = ramSize - 256 * 1024;
  new Uint8Array(memory.buffer).set(elfData, elfOffset);
  const vmPtr = createProcessWithPolicy(owner, elfOffset, elfData.length, ramSize, 1024 * 1024);
  expect(vmPtr).toBeGreaterThan(0);

  const ownerState = readKernelRuntimeState(owner);
  const guestBase = owner.thread_guest_base?.(vmPtr) ?? 0;
  const processPtr = owner.thread_process_ptr(vmPtr);
  const processStateBytes = owner.process_state_bytes();
  const processState = new Uint8Array(
    memory.buffer,
    guestBase + processPtr,
    processStateBytes,
  ).slice();

  const childMemory = new WebAssembly.Memory({
    initial: Math.ceil(Math.max(memory.buffer.byteLength, guestBase + 0x10000) / 65536),
    maximum: 32768,
    shared: true,
  });
  const childInstance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(childMemory),
  });
  const child = childInstance.exports as unknown as KernelExports;

  copyKernelPrivateRegion(memory, childMemory, ownerState.heapPtr);
  copyExecveKernelRuntimeRegions(owner, memory, childMemory);
  attachKernelRuntimeState(child, ownerState);

  const staged = stageExecveGuestHandoff(child, guestBase, {
    processState,
    path: cstr("/child"),
    argv: [cstr("/child")],
    envp: [],
  });
  setExecveMemoryHandoff(
    child,
    vmPtr,
    staged.processPtr,
    staged.pathPtr,
    staged.argvPtr,
    staged.envpPtr,
    ramSize,
  );

  expect(child.thread_complete_execve_handoff(vmPtr)).toBe(0n);
  expect(child.thread_status(vmPtr)).toBe(child.abi_status_ok());
});

test("shared-memory dual kernel instances preserve parent clone return after child futex wait", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports: owner, memory, module } = loaded;
  const childInstance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(memory),
  });
  const child = childInstance.exports as unknown as KernelExports;

  const elfData = findRiscvElf("add");
  if (!elfData) {
    logSkip("add.elf not found");
    return;
  }

  const heapBase = (owner as any).__heap_base?.value ?? 0x200000;
  owner.kernel_init(heapBase);
  const ramSize = memory.buffer.byteLength;
  const elfOffset = ramSize - 256 * 1024;
  new Uint8Array(memory.buffer).set(elfData, elfOffset);
  const vmPtr = createProcessWithPolicy(owner, elfOffset, elfData.length, ramSize, 1024 * 1024);
  expect(vmPtr).toBeGreaterThan(0);

  attachKernelRuntimeState(child, readKernelRuntimeState(owner));

  const guestBase = owner.thread_guest_base?.(vmPtr) ?? 0;
  const entryPc = Number(owner.thread_pc(vmPtr));
  const codeAddr = guestBase + entryPc;
  const helperPc = entryPc + 4;
  const returnPathPc = entryPc + 0x40;
  const childFnPc = entryPc + 0x80;
  const finalTargetPc = entryPc + 0xc0;
  const futexAddr = entryPc + 0x180;
  const parentSp = entryPc + 0x240;
  const childStack = entryPc + 0x300;

  writeU32(memory, codeAddr, 0x0000_0073);
  writeU32(memory, guestBase + helperPc, encodeB(4, 10, 0, 10));
  writeU16(memory, guestBase + helperPc + 4, 0xc511);
  writeU16(memory, guestBase + helperPc + 6, 0x8082);
  writeU16(memory, guestBase + helperPc + 10, 0x5529);
  writeU16(memory, guestBase + helperPc + 16, 0x8536);
  writeU16(memory, guestBase + helperPc + 18, 0x9602);
  writeU16(memory, guestBase + returnPathPc, 0x57fd);
  writeU32(memory, guestBase + returnPathPc + 2, encodeB(0, 10, 15, 12));
  writeU16(memory, guestBase + returnPathPc + 6, 0x60a2);
  writeU16(memory, guestBase + returnPathPc + 8, 0x6402);
  writeU16(memory, guestBase + returnPathPc + 10, 0x0141);
  writeU16(memory, guestBase + returnPathPc + 12, 0x8082);
  writeU32(memory, guestBase + childFnPc, encodeI(0x13, 11, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 4, encodeI(0x13, 12, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 8, encodeI(0x13, 13, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 12, encodeI(0x13, 14, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 16, encodeI(0x13, 15, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 20, encodeI(0x13, 17, 0, 0, SYS_FUTEX));
  writeU32(memory, guestBase + childFnPc + 24, 0x0000_0073);
  writeU32(memory, guestBase + finalTargetPc, encodeI(0x13, 15, 0, 0, 9));
  writeU32(memory, guestBase + futexAddr, 0);
  writeU64(memory, guestBase + parentSp, 0x28n);
  writeU64(memory, guestBase + parentSp + 8, BigInt(finalTargetPc));

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(returnPathPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(10), BigInt(
    CLONE_VM | CLONE_THREAD | CLONE_SIGHAND | CLONE_FS,
  ));
  writeU64(memory, vmPtr + threadRegOffset(11), BigInt(childStack));
  writeU64(memory, vmPtr + threadRegOffset(12), 0n);
  writeU64(memory, vmPtr + threadRegOffset(13), 0n);
  writeU64(memory, vmPtr + threadRegOffset(14), 0n);
  writeU64(memory, vmPtr + threadRegOffset(17), BigInt(SYS_CLONE));
  writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(entryPc));

  owner.thread_step(vmPtr, 1);
  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_clone_spawn());

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(returnPathPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(12), BigInt(childFnPc));
  writeU64(memory, vmPtr + threadRegOffset(13), BigInt(futexAddr));
  writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(helperPc));

  const childVmPtr = owner.thread_spawn_from_clone(vmPtr);
  expect(childVmPtr).toBeGreaterThan(0);

  attachKernelRuntimeState(child, readKernelRuntimeState(owner));
  child.thread_step(childVmPtr, 16);
  expect(child.thread_status(childVmPtr)).toBe(owner.abi_status_futex_wait());

  attachKernelRuntimeState(owner, readKernelRuntimeState(child));
  owner.thread_step(vmPtr, 9);

  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_ok());
  expect(owner.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc));
  expect(readThreadRa(owner, memory, vmPtr)).toBe(BigInt(finalTargetPc));
  expect(owner.thread_sp(vmPtr)).toBe(BigInt(parentSp + 16));
});

test("shared-memory dual kernel instances materialize fs-ring openat fd table entries", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports: owner, memory, module } = loaded;
  const workerInstance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(memory),
  });
  const worker = workerInstance.exports as unknown as KernelExports;

  const elfData = findRiscvElf("add");
  if (!elfData) {
    logSkip("add.elf not found");
    return;
  }

  const heapBase = (owner as any).__heap_base?.value ?? 0x200000;
  owner.kernel_init(heapBase);
  const ramSize = memory.buffer.byteLength;
  const elfOffset = ramSize - 256 * 1024;
  new Uint8Array(memory.buffer).set(elfData, elfOffset);
  const vmPtr = createProcessWithPolicy(owner, elfOffset, elfData.length, ramSize, 1024 * 1024);
  expect(vmPtr).toBeGreaterThan(0);
  attachKernelRuntimeState(worker, readKernelRuntimeState(owner));

  const guestBase = owner.thread_guest_base?.(vmPtr) ?? 0;
  const path = cstr("/worker-openat-created.txt");
  const pathAddr = Number(owner.debug_memory_brk_current?.() ?? 0n) + 0x1000;
  ensureMemoryCapacity(memory, guestBase + pathAddr + path.length);
  new Uint8Array(memory.buffer).set(path, guestBase + pathAddr);

  const ringBase = owner.thread_fs_ring_base?.(vmPtr) ?? 0;
  expect(ringBase).toBeGreaterThan(0);
  const view = new DataView(memory.buffer);
  const request = ringBase + FS_RING_HEADER_BYTES;
  view.setUint32(ringBase + FS_RING_SUBMIT_HEAD_OFFSET, 0, true);
  view.setUint32(ringBase + FS_RING_SUBMIT_TAIL_OFFSET, 0, true);
  view.setUint32(request, FS_OP_OPENAT, true);
  view.setInt32(request + 4, AT_FDCWD, true);
  view.setBigUint64(request + 8, 0n, true);
  view.setBigUint64(request + FS_RING_REQUEST_LEN_OFFSET, 0n, true);
  view.setBigUint64(request + FS_RING_REQUEST_BUF_PTR_OFFSET, 0n, true);
  view.setBigUint64(request + FS_RING_REQUEST_PATH_PTR_OFFSET, BigInt(pathAddr), true);
  view.setUint32(request + 40, O_RDONLY | O_CREAT, true);
  view.setUint32(request + 44, 0o644, true);
  view.setUint32(request + FS_RING_REQUEST_VM_PTR_OFFSET, vmPtr, true);
  view.setUint32(ringBase + FS_RING_SUBMIT_HEAD_OFFSET, 1, true);

  worker.thread_process_fs(vmPtr);
  const fd = Number(worker.thread_resume_fs(vmPtr));

  expect(fd).toBeGreaterThanOrEqual(3);
  expect(worker.process_fd_ofd_id(vmPtr, fd)).toBeGreaterThan(0);
  expect(owner.process_fd_ofd_id(vmPtr, fd)).toBeGreaterThan(0);
});

test("shared imported memory materializes a top-of-limit prot-none page without early ENOMEM", async () => {
  const maximumPages = 64512;
  const memory = new WebAssembly.Memory({
    initial: 1024,
    maximum: maximumPages,
    shared: true,
  });
  const loaded = await loadKernel({ memory });
  if (!loaded) {
    return;
  }
  const { exports, memory: wasmMemory } = loaded;

  const elfData = findRiscvElf("add");
  if (!elfData) {
    logSkip("add.elf not found");
    return;
  }

  const heapBase = (exports as any).__heap_base?.value ?? 0x200000;
  exports.kernel_init(heapBase);

  const ramSize = wasmMemory.buffer.byteLength;
  const elfOffset = ramSize - 256 * 1024;
  new Uint8Array(wasmMemory.buffer).set(elfData, elfOffset);
  const vmPtr = createProcessWithPolicy(exports, elfOffset, elfData.length, ramSize, 1024 * 1024);
  expect(vmPtr).toBeGreaterThan(0);

  const guestBase = exports.thread_guest_base?.(vmPtr) ?? 0;
  const maxBytes = maximumPages * 65536;
  const guestRamLimit = (maxBytes - guestBase) >>> 0;
  exports.thread_set_guest_memory_policy_limit?.(vmPtr, BigInt(guestRamLimit));
  expect(exports.thread_guest_memory_policy_limit?.(vmPtr) ?? 0n).toBe(BigInt(guestRamLimit));

  const entryPc = Number(exports.thread_pc(vmPtr));
  const codeAddr = guestBase + entryPc;
  const finalTargetPc = entryPc + 0x40;
  const parentSp = entryPc + 0x240;
  const reserveLen = 0x20_000;
  const reserveAddr = guestRamLimit - reserveLen;
  const topPageAddr = guestRamLimit - 0x1000;

  writeU32(wasmMemory, codeAddr, 0x0000_0073);
  writeU32(wasmMemory, guestBase + entryPc + 4, 0x0000_8067);
  writeU32(wasmMemory, guestBase + finalTargetPc, 0x0000_0073);
  writeU32(wasmMemory, guestBase + finalTargetPc + 4, 0x0000_8067);
  writeU16(wasmMemory, guestBase + finalTargetPc + 8, 0x0001);

  writeU64(wasmMemory, vmPtr + threadRegOffset(1), BigInt(finalTargetPc));
  writeU64(wasmMemory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(wasmMemory, vmPtr + threadRegOffset(10), BigInt(reserveAddr));
  writeU64(wasmMemory, vmPtr + threadRegOffset(11), BigInt(reserveLen));
  writeU64(wasmMemory, vmPtr + threadRegOffset(12), BigInt(PROT_NONE));
  writeU64(wasmMemory, vmPtr + threadRegOffset(13), BigInt(MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED));
  writeU64(wasmMemory, vmPtr + threadRegOffset(14), BigInt(-1));
  writeU64(wasmMemory, vmPtr + threadRegOffset(15), 0n);
  writeU64(wasmMemory, vmPtr + threadRegOffset(17), BigInt(SYS_MMAP));
  writeU64(wasmMemory, vmPtr + threadRegOffset(27), 0x34567890n);
  writeU64(wasmMemory, vmPtr + THREAD_PC_OFFSET, BigInt(entryPc));

  exports.thread_step(vmPtr, 2);
  expect(exports.thread_status(vmPtr)).toBe(exports.abi_status_ok());
  expect(exports.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc));
  expect(exports.thread_sp(vmPtr)).toBe(BigInt(parentSp));
  expect(readThreadReg(exports, wasmMemory, vmPtr, 10)).toBe(BigInt(reserveAddr));

  writeU64(wasmMemory, vmPtr + threadRegOffset(1), BigInt(finalTargetPc + 8));
  writeU64(wasmMemory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(wasmMemory, vmPtr + threadRegOffset(10), BigInt(topPageAddr));
  writeU64(wasmMemory, vmPtr + threadRegOffset(11), 0x1000n);
  writeU64(wasmMemory, vmPtr + threadRegOffset(12), BigInt(PROT_READ | PROT_WRITE));
  writeU64(wasmMemory, vmPtr + threadRegOffset(17), BigInt(SYS_MPROTECT));
  writeU64(wasmMemory, vmPtr + threadRegOffset(27), 0x34567890n);
  writeU64(wasmMemory, vmPtr + THREAD_PC_OFFSET, BigInt(finalTargetPc));

  exports.thread_step(vmPtr, 2);

  expect(exports.thread_status(vmPtr)).toBe(exports.abi_status_ok());
  expect(exports.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc + 8));
  expect(exports.thread_sp(vmPtr)).toBe(BigInt(parentSp));
  expect(readThreadReg(exports, wasmMemory, vmPtr, 27)).toBe(0x34567890n);
  expect(readThreadReg(exports, wasmMemory, vmPtr, 10)).toBe(0n);
  expect(wasmMemory.buffer.byteLength).toBe(maxBytes);
  const topPage = new Uint8Array(wasmMemory.buffer, guestBase + topPageAddr, 0x1000);
  expect(Array.from(topPage).every((byte) => byte === 0)).toBe(true);
});

test("shared-memory dual kernel instances preserve parent context across clone and parent mprotect materialization", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports: owner, memory, module } = loaded;
  const childInstance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(memory),
  });
  const child = childInstance.exports as unknown as KernelExports;

  const elfData = findRiscvElf("add");
  if (!elfData) {
    logSkip("add.elf not found");
    return;
  }

  const heapBase = (owner as any).__heap_base?.value ?? 0x200000;
  owner.kernel_init(heapBase);
  const ramSize = memory.buffer.byteLength;
  const elfOffset = ramSize - 256 * 1024;
  new Uint8Array(memory.buffer).set(elfData, elfOffset);
  const vmPtr = createProcessWithPolicy(owner, elfOffset, elfData.length, ramSize, 1024 * 1024);
  expect(vmPtr).toBeGreaterThan(0);

  attachKernelRuntimeState(child, readKernelRuntimeState(owner));

  const guestBase = owner.thread_guest_base?.(vmPtr) ?? 0;
  const entryPc = Number(owner.thread_pc(vmPtr));
  const codeAddr = guestBase + entryPc;
  const helperPc = entryPc + 4;
  const returnPathPc = entryPc + 0x40;
  const childFnPc = entryPc + 0x80;
  const finalTargetPc = entryPc + 0xc0;
  const futexAddr = entryPc + 0x180;
  const parentSp = entryPc + 0x240;
  const childStack = entryPc + 0x300;
  const reserveAddr = entryPc + 0x4000;
  const reserveLen = 0x3000;

  writeU32(memory, codeAddr, 0x0000_0073);
  writeU32(memory, guestBase + entryPc + 4, 0x0000_8067);

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(finalTargetPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(10), BigInt(reserveAddr));
  writeU64(memory, vmPtr + threadRegOffset(11), BigInt(reserveLen));
  writeU64(memory, vmPtr + threadRegOffset(12), BigInt(PROT_NONE));
  writeU64(memory, vmPtr + threadRegOffset(13), BigInt(MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED));
  writeU64(memory, vmPtr + threadRegOffset(14), BigInt(-1));
  writeU64(memory, vmPtr + threadRegOffset(15), 0n);
  writeU64(memory, vmPtr + threadRegOffset(17), BigInt(SYS_MMAP));
  writeU64(memory, vmPtr + threadRegOffset(27), 0x34567890n);
  writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(entryPc));

  owner.thread_step(vmPtr, 2);
  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_ok());
  expect(owner.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc));
  expect(owner.thread_exit_code(vmPtr)).toBe(0);
  expect(owner.thread_sp(vmPtr)).toBe(BigInt(parentSp));
  expect(readThreadReg(owner, memory, vmPtr, 10)).toBe(BigInt(reserveAddr));

  writeU32(memory, guestBase + helperPc, encodeB(4, 10, 0, 10));
  writeU16(memory, guestBase + helperPc + 4, 0xc511);
  writeU16(memory, guestBase + helperPc + 6, 0x8082);
  writeU16(memory, guestBase + helperPc + 10, 0x5529);
  writeU16(memory, guestBase + helperPc + 16, 0x8536);
  writeU16(memory, guestBase + helperPc + 18, 0x9602);
  writeU16(memory, guestBase + returnPathPc, 0x57fd);
  writeU32(memory, guestBase + returnPathPc + 2, encodeB(0, 10, 15, 12));
  writeU16(memory, guestBase + returnPathPc + 6, 0x60a2);
  writeU16(memory, guestBase + returnPathPc + 8, 0x6402);
  writeU16(memory, guestBase + returnPathPc + 10, 0x0141);
  writeU16(memory, guestBase + returnPathPc + 12, 0x8082);
  writeU32(memory, guestBase + childFnPc, encodeI(0x13, 11, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 4, encodeI(0x13, 12, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 8, encodeI(0x13, 13, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 12, encodeI(0x13, 14, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 16, encodeI(0x13, 15, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 20, encodeI(0x13, 17, 0, 0, SYS_FUTEX));
  writeU32(memory, guestBase + childFnPc + 24, 0x0000_0073);
  writeU32(memory, guestBase + futexAddr, 0);
  writeU32(memory, guestBase + finalTargetPc, 0x0000_0073);
  writeU32(memory, guestBase + finalTargetPc + 4, 0x0000_8067);
  writeU16(memory, guestBase + finalTargetPc + 8, 0x0001);
  writeU64(memory, guestBase + parentSp, 0x28n);
  writeU64(memory, guestBase + parentSp + 8, BigInt(finalTargetPc));

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(returnPathPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(10), BigInt(
    CLONE_VM | CLONE_THREAD | CLONE_SIGHAND | CLONE_FS,
  ));
  writeU64(memory, vmPtr + threadRegOffset(11), BigInt(childStack));
  writeU64(memory, vmPtr + threadRegOffset(12), 0n);
  writeU64(memory, vmPtr + threadRegOffset(13), 0n);
  writeU64(memory, vmPtr + threadRegOffset(14), 0n);
  writeU64(memory, vmPtr + threadRegOffset(17), BigInt(SYS_CLONE));
  writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(entryPc));

  owner.thread_step(vmPtr, 1);
  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_clone_spawn());

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(returnPathPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(12), BigInt(childFnPc));
  writeU64(memory, vmPtr + threadRegOffset(13), BigInt(futexAddr));

  const childVmPtr = owner.thread_spawn_from_clone(vmPtr);
  expect(childVmPtr).toBeGreaterThan(0);

  attachKernelRuntimeState(child, readKernelRuntimeState(owner));
  child.thread_step(childVmPtr, 16);
  expect(child.thread_status(childVmPtr)).toBe(owner.abi_status_futex_wait());

  attachKernelRuntimeState(owner, readKernelRuntimeState(child));
  owner.thread_step(vmPtr, 9);
  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_ok());
  expect(owner.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc));
  expect(readThreadRa(owner, memory, vmPtr)).toBe(BigInt(finalTargetPc));
  expect(owner.thread_sp(vmPtr)).toBe(BigInt(parentSp + 16));

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(finalTargetPc + 8));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp + 16));
  writeU64(memory, vmPtr + threadRegOffset(10), BigInt(reserveAddr));
  writeU64(memory, vmPtr + threadRegOffset(11), BigInt(reserveLen));
  writeU64(memory, vmPtr + threadRegOffset(12), BigInt(PROT_READ | PROT_WRITE));
  writeU64(memory, vmPtr + threadRegOffset(17), BigInt(SYS_MPROTECT));
  writeU64(memory, vmPtr + threadRegOffset(27), 0x34567890n);
  writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(finalTargetPc));

  attachKernelRuntimeState(owner, readKernelRuntimeState(child));
  owner.thread_step(vmPtr, 2);

  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_ok());
  expect(owner.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc + 8));
  expect(owner.thread_sp(vmPtr)).toBe(BigInt(parentSp + 16));
  expect(readThreadReg(owner, memory, vmPtr, 27)).toBe(0x34567890n);
  expect(readThreadReg(owner, memory, vmPtr, 10)).toBe(0n);
});

test("shared-memory dual kernel instances preserve parent context across clone, flush_icache, and re-exec", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports: owner, memory, module } = loaded;
  const childInstance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(memory),
  });
  const child = childInstance.exports as unknown as KernelExports;

  const elfData = findRiscvElf("add");
  if (!elfData) {
    logSkip("add.elf not found");
    return;
  }

  const heapBase = (owner as any).__heap_base?.value ?? 0x200000;
  owner.kernel_init(heapBase);
  const ramSize = memory.buffer.byteLength;
  const elfOffset = ramSize - 256 * 1024;
  new Uint8Array(memory.buffer).set(elfData, elfOffset);
  const vmPtr = createProcessWithPolicy(owner, elfOffset, elfData.length, ramSize, 1024 * 1024);
  expect(vmPtr).toBeGreaterThan(0);

  attachKernelRuntimeState(child, readKernelRuntimeState(owner));

  const guestBase = owner.thread_guest_base?.(vmPtr) ?? 0;
  const entryPc = Number(owner.thread_pc(vmPtr));
  const codeAddr = guestBase + entryPc;
  const helperPc = entryPc + 4;
  const returnPathPc = entryPc + 0x40;
  const childFnPc = entryPc + 0x80;
  const finalTargetPc = entryPc + 0xc0;
  const futexAddr = entryPc + 0x180;
  const parentSp = entryPc + 0x240;
  const childStack = entryPc + 0x300;
  const codePageAddr = entryPc + 0x4000;
  const codePageLen = 0x1000;

  writeU32(memory, codeAddr, 0x0000_0073);
  writeU32(memory, guestBase + entryPc + 4, 0x0000_8067);

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(finalTargetPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(10), BigInt(codePageAddr));
  writeU64(memory, vmPtr + threadRegOffset(11), BigInt(codePageLen));
  writeU64(memory, vmPtr + threadRegOffset(12), BigInt(PROT_NONE));
  writeU64(memory, vmPtr + threadRegOffset(13), BigInt(MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED));
  writeU64(memory, vmPtr + threadRegOffset(14), BigInt(-1));
  writeU64(memory, vmPtr + threadRegOffset(15), 0n);
  writeU64(memory, vmPtr + threadRegOffset(17), BigInt(SYS_MMAP));
  writeU64(memory, vmPtr + threadRegOffset(27), 0x34567890n);
  writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(entryPc));

  owner.thread_step(vmPtr, 2);
  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_ok());
  expect(owner.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc));
  expect(owner.thread_exit_code(vmPtr)).toBe(0);
  expect(owner.thread_sp(vmPtr)).toBe(BigInt(parentSp));
  expect(readThreadReg(owner, memory, vmPtr, 10)).toBe(BigInt(codePageAddr));

  writeU32(memory, guestBase + helperPc, encodeB(4, 10, 0, 10));
  writeU16(memory, guestBase + helperPc + 4, 0xc511);
  writeU16(memory, guestBase + helperPc + 6, 0x8082);
  writeU16(memory, guestBase + helperPc + 10, 0x5529);
  writeU16(memory, guestBase + helperPc + 16, 0x8536);
  writeU16(memory, guestBase + helperPc + 18, 0x9602);
  writeU16(memory, guestBase + returnPathPc, 0x57fd);
  writeU32(memory, guestBase + returnPathPc + 2, encodeB(0, 10, 15, 12));
  writeU16(memory, guestBase + returnPathPc + 6, 0x60a2);
  writeU16(memory, guestBase + returnPathPc + 8, 0x6402);
  writeU16(memory, guestBase + returnPathPc + 10, 0x0141);
  writeU16(memory, guestBase + returnPathPc + 12, 0x8082);
  writeU32(memory, guestBase + childFnPc, encodeI(0x13, 11, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 4, encodeI(0x13, 12, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 8, encodeI(0x13, 13, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 12, encodeI(0x13, 14, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 16, encodeI(0x13, 15, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 20, encodeI(0x13, 17, 0, 0, SYS_FUTEX));
  writeU32(memory, guestBase + childFnPc + 24, 0x0000_0073);
  writeU32(memory, guestBase + futexAddr, 0);
  writeU32(memory, guestBase + finalTargetPc, 0x0000_0073);
  writeU32(memory, guestBase + finalTargetPc + 4, 0x0000_8067);
  writeU16(memory, guestBase + finalTargetPc + 8, 0x0001);
  writeU64(memory, guestBase + parentSp, 0x28n);
  writeU64(memory, guestBase + parentSp + 8, BigInt(finalTargetPc));

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(returnPathPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(10), BigInt(
    CLONE_VM | CLONE_THREAD | CLONE_SIGHAND | CLONE_FS,
  ));
  writeU64(memory, vmPtr + threadRegOffset(11), BigInt(childStack));
  writeU64(memory, vmPtr + threadRegOffset(12), 0n);
  writeU64(memory, vmPtr + threadRegOffset(13), 0n);
  writeU64(memory, vmPtr + threadRegOffset(14), 0n);
  writeU64(memory, vmPtr + threadRegOffset(17), BigInt(SYS_CLONE));
  writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(entryPc));

  owner.thread_step(vmPtr, 1);
  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_clone_spawn());

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(returnPathPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(12), BigInt(childFnPc));
  writeU64(memory, vmPtr + threadRegOffset(13), BigInt(futexAddr));

  const childVmPtr = owner.thread_spawn_from_clone(vmPtr);
  expect(childVmPtr).toBeGreaterThan(0);

  attachKernelRuntimeState(child, readKernelRuntimeState(owner));
  child.thread_step(childVmPtr, 16);
  expect(child.thread_status(childVmPtr)).toBe(owner.abi_status_futex_wait());

  attachKernelRuntimeState(owner, readKernelRuntimeState(child));
  owner.thread_step(vmPtr, 9);
  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_ok());
  expect(owner.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc));
  expect(readThreadRa(owner, memory, vmPtr)).toBe(BigInt(finalTargetPc));
  expect(owner.thread_sp(vmPtr)).toBe(BigInt(parentSp + 16));

  const runParentSyscall = (sysno: number, a0: number, a1: number, a2: number, s11: bigint) => {
    writeU64(memory, vmPtr + threadRegOffset(1), BigInt(finalTargetPc + 8));
    writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp + 16));
    writeU64(memory, vmPtr + threadRegOffset(10), BigInt(a0));
    writeU64(memory, vmPtr + threadRegOffset(11), BigInt(a1));
    writeU64(memory, vmPtr + threadRegOffset(12), BigInt(a2));
    writeU64(memory, vmPtr + threadRegOffset(17), BigInt(sysno));
    writeU64(memory, vmPtr + threadRegOffset(27), s11);
    writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(finalTargetPc));

    owner.thread_step(vmPtr, 2);

    expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_ok());
    expect(owner.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc + 8));
    expect(owner.thread_sp(vmPtr)).toBe(BigInt(parentSp + 16));
    expect(readThreadReg(owner, memory, vmPtr, 27)).toBe(s11);
    expect(readThreadReg(owner, memory, vmPtr, 10)).toBe(0n);
  };

  const runParentCode = (expectedA0: bigint, s11: bigint) => {
    writeU64(memory, vmPtr + threadRegOffset(1), BigInt(codePageAddr + 8));
    writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp + 16));
    writeU64(memory, vmPtr + threadRegOffset(10), 0n);
    writeU64(memory, vmPtr + threadRegOffset(27), s11);
    writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(codePageAddr));

    owner.thread_step(vmPtr, 2);

    expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_ok());
    expect(owner.thread_pc(vmPtr)).toBe(BigInt(codePageAddr + 8));
    expect(owner.thread_sp(vmPtr)).toBe(BigInt(parentSp + 16));
    expect(readThreadReg(owner, memory, vmPtr, 10)).toBe(expectedA0);
    expect(readThreadReg(owner, memory, vmPtr, 27)).toBe(s11);
  };

  runParentSyscall(SYS_MPROTECT, codePageAddr, codePageLen, PROT_READ | PROT_WRITE, 0x34567890n);
  writeU32(memory, guestBase + codePageAddr, 0x0010_0513);
  writeU32(memory, guestBase + codePageAddr + 4, 0x0000_8067);
  writeU16(memory, guestBase + codePageAddr + 8, 0x0001);
  runParentSyscall(SYS_RISCV_FLUSH_ICACHE, codePageAddr, codePageAddr + 8, 0, 0x34567891n);
  runParentSyscall(SYS_MPROTECT, codePageAddr, codePageLen, PROT_READ | PROT_EXEC, 0x34567892n);
  runParentCode(1n, 0x34567893n);

  runParentSyscall(SYS_MPROTECT, codePageAddr, codePageLen, PROT_READ | PROT_WRITE, 0x34567894n);
  writeU32(memory, guestBase + codePageAddr, 0x02a0_0513);
  runParentSyscall(SYS_RISCV_FLUSH_ICACHE, codePageAddr, codePageAddr + 8, 0, 0x34567895n);
  runParentSyscall(SYS_MPROTECT, codePageAddr, codePageLen, PROT_READ | PROT_EXEC, 0x34567896n);
  runParentCode(42n, 0x34567897n);
});

test("shared-memory dual kernel instances preserve parent fault snapshot with blocked sibling", async () => {
  const loaded = await loadKernel();
  if (!loaded) {
    return;
  }
  const { exports: owner, memory, module } = loaded;
  const childInstance = await WebAssembly.instantiate(module, {
    env: createKernelEnv(memory),
  });
  const child = childInstance.exports as unknown as KernelExports;

  const elfData = findRiscvElf("add");
  if (!elfData) {
    logSkip("add.elf not found");
    return;
  }

  const heapBase = (owner as any).__heap_base?.value ?? 0x200000;
  owner.kernel_init(heapBase);
  const ramSize = memory.buffer.byteLength;
  const elfOffset = ramSize - 256 * 1024;
  new Uint8Array(memory.buffer).set(elfData, elfOffset);
  const vmPtr = createProcessWithPolicy(owner, elfOffset, elfData.length, ramSize, 1024 * 1024);
  expect(vmPtr).toBeGreaterThan(0);

  attachKernelRuntimeState(child, readKernelRuntimeState(owner));

  const guestBase = owner.thread_guest_base?.(vmPtr) ?? 0;
  const entryPc = Number(owner.thread_pc(vmPtr));
  const codeAddr = guestBase + entryPc;
  const helperPc = entryPc + 4;
  const returnPathPc = entryPc + 0x40;
  const childFnPc = entryPc + 0x80;
  const finalTargetPc = entryPc + 0xc0;
  const futexAddr = entryPc + 0x180;
  const parentSp = entryPc + 0x240;
  const childStack = entryPc + 0x300;

  writeU32(memory, codeAddr, 0x0000_0073);
  writeU32(memory, guestBase + entryPc + 4, 0x0000_8067);
  writeU32(memory, guestBase + helperPc, encodeB(4, 10, 0, 10));
  writeU16(memory, guestBase + helperPc + 4, 0xc511);
  writeU16(memory, guestBase + helperPc + 6, 0x8082);
  writeU16(memory, guestBase + helperPc + 10, 0x5529);
  writeU16(memory, guestBase + helperPc + 16, 0x8536);
  writeU16(memory, guestBase + helperPc + 18, 0x9602);
  writeU16(memory, guestBase + returnPathPc, 0x57fd);
  writeU32(memory, guestBase + returnPathPc + 2, encodeB(0, 10, 15, 12));
  writeU16(memory, guestBase + returnPathPc + 6, 0x60a2);
  writeU16(memory, guestBase + returnPathPc + 8, 0x6402);
  writeU16(memory, guestBase + returnPathPc + 10, 0x0141);
  writeU16(memory, guestBase + returnPathPc + 12, 0x8082);
  writeU32(memory, guestBase + childFnPc, encodeI(0x13, 11, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 4, encodeI(0x13, 12, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 8, encodeI(0x13, 13, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 12, encodeI(0x13, 14, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 16, encodeI(0x13, 15, 0, 0, 0));
  writeU32(memory, guestBase + childFnPc + 20, encodeI(0x13, 17, 0, 0, SYS_FUTEX));
  writeU32(memory, guestBase + childFnPc + 24, 0x0000_0073);
  writeU32(memory, guestBase + futexAddr, 0);
  writeU32(memory, guestBase + finalTargetPc, 0x0000_0073);
  writeU32(memory, guestBase + finalTargetPc + 4, 0x0000_8067);
  writeU16(memory, guestBase + finalTargetPc + 8, 0x0001);
  writeU64(memory, guestBase + parentSp, 0x28n);
  writeU64(memory, guestBase + parentSp + 8, BigInt(finalTargetPc));

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(returnPathPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(10), BigInt(
    CLONE_VM | CLONE_THREAD | CLONE_SIGHAND | CLONE_FS,
  ));
  writeU64(memory, vmPtr + threadRegOffset(11), BigInt(childStack));
  writeU64(memory, vmPtr + threadRegOffset(12), 0n);
  writeU64(memory, vmPtr + threadRegOffset(13), 0n);
  writeU64(memory, vmPtr + threadRegOffset(14), 0n);
  writeU64(memory, vmPtr + threadRegOffset(17), BigInt(SYS_CLONE));
  writeU64(memory, vmPtr + THREAD_PC_OFFSET, BigInt(entryPc));

  owner.thread_step(vmPtr, 1);
  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_clone_spawn());

  writeU64(memory, vmPtr + threadRegOffset(1), BigInt(returnPathPc));
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp));
  writeU64(memory, vmPtr + threadRegOffset(12), BigInt(childFnPc));
  writeU64(memory, vmPtr + threadRegOffset(13), BigInt(futexAddr));

  const childVmPtr = owner.thread_spawn_from_clone(vmPtr);
  expect(childVmPtr).toBeGreaterThan(0);

  attachKernelRuntimeState(child, readKernelRuntimeState(owner));
  child.thread_step(childVmPtr, 16);
  expect(child.thread_status(childVmPtr)).toBe(owner.abi_status_futex_wait());

  attachKernelRuntimeState(owner, readKernelRuntimeState(child));
  owner.thread_step(vmPtr, 9);
  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_ok());
  expect(owner.thread_pc(vmPtr)).toBe(BigInt(finalTargetPc));
  expect(owner.thread_sp(vmPtr)).toBe(BigInt(parentSp + 16));

  writeU64(memory, vmPtr + threadRegOffset(1), 0n);
  writeU64(memory, vmPtr + threadRegOffset(2), BigInt(parentSp + 16));
  writeU64(memory, vmPtr + threadRegOffset(27), 0x34567898n);
  writeU64(memory, vmPtr + THREAD_PC_OFFSET, 4n);

  owner.thread_step(vmPtr, 1);

  expect(owner.thread_status(vmPtr)).toBe(owner.abi_status_fault());
  expect(readThreadFaultPc(owner, memory, vmPtr)).toBe(4n);
  expect(readThreadFaultAddr(owner, memory, vmPtr)).toBe(4n);
  expect(owner.thread_term_signal(vmPtr)).toBe(SIGSEGV);
});
