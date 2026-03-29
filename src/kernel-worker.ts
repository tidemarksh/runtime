/**
 * Kernel Worker — dedicated Web Worker hosting the kernel WASM instance.
 *
 * §6.1: kernel Worker owns private WebAssembly.Memory containing:
 *   - inode metadata, directory tree
 *   - ProcessState[] (OFD, VMA, epoll, socket, signal)
 *   - pipe control (refcount, wait queue)
 *
 * §6.3: Processes FS ring buffer requests from thread-workers:
 *   thread-worker → SAB ring buffer submit → Atomics.wait
 *   kernel Worker → read SQ → process_pending_requests → write CQ → Atomics.notify
 *
 * §6.4: Process lifecycle handoff — Worker creation is delegated to the
 *   main thread (TS) since WASM cannot call the Web Worker API.
 */

import type { KernelExports, KernelRuntimeState } from "./abi.js";
import { readKernelRuntimeState } from "./abi.js";
import type { KernelWorkerInbound, KernelWorkerOutbound } from "./messages.js";

let exports: KernelExports;

/**
 * Initialize the kernel WASM instance with private memory.
 */
async function initKernel(wasmModule: WebAssembly.Module): Promise<KernelRuntimeState> {
  const instance = await WebAssembly.instantiate(wasmModule, {});
  exports = instance.exports as unknown as KernelExports;

  const heapBase = 0x200000; // 2 MB
  exports.kernel_init(heapBase);

  // Set wall clock base time to current time
  const now = Date.now();
  const sec = BigInt(Math.floor(now / 1000));
  const nsec = BigInt((now % 1000) * 1_000_000);
  exports.set_base_time(sec, nsec);

  return readKernelRuntimeState(exports);
}

/**
 * Write a file to MemFS via kernel WASM exports.
 * Path components are created as needed (mkdir -p equivalent).
 */
function writeFile(path: string, data: Uint8Array): void {
  const encoder = new TextEncoder();
  const mem = new Uint8Array(exports.memory.buffer);

  // Scratch area in kernel Worker's private WASM memory
  const scratchBase = exports.kernel_heap_ptr() + 0x10000;

  // Parse path into components
  const parts = path.split("/").filter(Boolean);
  const filename = parts.pop();
  if (!filename) throw new Error(`Invalid path: ${path}`);

  // Ensure parent directories exist
  let parentIno = 1; // root inode
  for (const dir of parts) {
    const dirBytes = encoder.encode(dir);
    mem.set(dirBytes, scratchBase);
    parentIno = exports.memfs_ensure_dir(parentIno, scratchBase, dirBytes.length, 0o755);
    if (parentIno === 0) throw new Error(`Failed to create directory: ${dir}`);
  }

  // Create file with data
  const nameBytes = encoder.encode(filename);
  mem.set(nameBytes, scratchBase);
  const dataOffset = scratchBase + nameBytes.length + 16;
  mem.set(data, dataOffset);

  const ino = exports.memfs_create_file_with_data(
    parentIno,
    scratchBase,
    nameBytes.length,
    dataOffset,
    data.length,
  );
  if (ino === 0) throw new Error(`Failed to write file: ${path}`);
}

/**
 * Read a file from MemFS via kernel WASM exports.
 */
function readFile(path: string): Uint8Array {
  const encoder = new TextEncoder();
  const mem = new Uint8Array(exports.memory.buffer);
  const scratchBase = exports.kernel_heap_ptr() + 0x10000;

  const pathBytes = encoder.encode(path);
  mem.set(pathBytes, scratchBase);

  const ino = exports.memfs_resolve_path(scratchBase, pathBytes.length);
  if (ino === 0) throw new Error(`File not found: ${path}`);

  const size = Number(exports.memfs_file_size(ino));
  const bufOffset = scratchBase + 1024;
  const bytesRead = Number(exports.memfs_read_file_to(ino, 0n, bufOffset, size));
  if (bytesRead < 0) throw new Error(`Failed to read file: ${path}`);

  return new Uint8Array(mem.buffer.slice(bufOffset, bufOffset + bytesRead));
}

/**
 * Create a process: copy ELF to kernel memory, push argv/env, call process_create.
 */
function createProcessInKernel(
  elfData: Uint8Array,
  argv: string[],
  env: string[],
  ramSize: number,
): { vmPtr: number; pid: number } {
  const mem = new Uint8Array(exports.memory.buffer);
  const encoder = new TextEncoder();
  const scratchBase = exports.kernel_heap_ptr() + 0x10000;

  // Copy ELF to kernel memory
  const elfOffset = scratchBase;
  mem.set(elfData, elfOffset);

  // Push argv
  exports.process_clear_args();
  let strOffset = elfOffset + elfData.length + 4096;
  for (const arg of argv) {
    const bytes = encoder.encode(arg + "\0");
    mem.set(bytes, strOffset);
    exports.process_push_arg(strOffset);
    strOffset += bytes.length;
  }

  // Push env
  for (const e of env) {
    const bytes = encoder.encode(e + "\0");
    mem.set(bytes, strOffset);
    exports.process_push_env(strOffset);
    strOffset += bytes.length;
  }

  // Create process
  const stackSize = 1024 * 1024; // 1 MB
  const vmPtr = exports.process_create(elfOffset, elfData.length, ramSize, stackSize);
  if (vmPtr === 0) throw new Error("process_create failed");

  const pid = 1; // TODO: read from kernel
  return { vmPtr, pid };
}

function postResponse(msg: KernelWorkerOutbound): void {
  postMessage(msg);
}

// Message handler
self.onmessage = async (event: MessageEvent<KernelWorkerInbound>) => {
  const msg = event.data;

  switch (msg.type) {
    case "init-kernel": {
      const kernelState = await initKernel(msg.wasmModule);
      postResponse({ type: "ready", kernelState });
      break;
    }

    case "create-process": {
      const { vmPtr, pid } = createProcessInKernel(
        msg.elfData,
        msg.argv,
        msg.env,
        msg.ramSize,
      );
      const kernelState = readKernelRuntimeState(exports);
      postResponse({ type: "process-created", vmPtr, pid, kernelState });
      break;
    }

    case "write-file": {
      writeFile(msg.path, msg.data);
      postResponse({ type: "file-written" });
      break;
    }

    case "read-file": {
      const data = readFile(msg.path);
      postResponse({ type: "file-read", data });
      break;
    }

    case "child-exit": {
      // TODO: record child exit for wait4 blocked threads
      break;
    }
  }
};
