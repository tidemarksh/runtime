import { expect, test } from "vite-plus/test";
import type { KernelExports } from "../../../src/abi.js";
import {
  findRiscvElf,
  readKernelWasm,
  Runtime,
} from "../../support/runtime-workload.js";

test("Runtime.spawn preserves seeded files in process MemFS", async () => {
  const wasmBytes = readKernelWasm();
  const elfData = findRiscvElf("add");
  if (!wasmBytes || !elfData) {
    return;
  }

  let runtime: InstanceType<typeof Runtime> | null = null;

  try {
    runtime = await Runtime.create({
      wasmBytes,
      ramSize: 64 * 1024 * 1024,
    });

    await runtime.fs.mkdir("/work", 0o755);
    await runtime.fs.mkdir("/work/hello", 0o755);
    await runtime.fs.writeFile(
      "/work/hello/module.txt",
      new TextEncoder().encode("hello from tidemark\n"),
      0o644,
    );

    const proc = await runtime.spawn(elfData, ["add"], [], {
      cwd: "/work/hello",
    });
    await expect(proc.waitForExit()).resolves.toBe(0);

    const handle = (proc as unknown as { handle: {
      kernel: KernelExports;
      memory: WebAssembly.Memory;
      guestBase: number;
      vmPtr: number;
    } }).handle;
    const encoder = new TextEncoder();
    const scratch = handle.kernel.kernel_heap_ptr() + 0x30000;
    const pathBytes = encoder.encode("/work/hello/module.txt");
    new Uint8Array(handle.memory.buffer).set(pathBytes, scratch);

    const ino = handle.kernel.memfs_resolve_path(scratch, pathBytes.length);
    const cwdBytes = encoder.encode("/work/hello");
    new Uint8Array(handle.memory.buffer).set(cwdBytes, scratch);
    const cwdIno = handle.kernel.memfs_resolve_path(scratch, cwdBytes.length);
    expect(ino).toBeGreaterThan(0);
    expect(cwdIno).toBeGreaterThan(0);
    expect(handle.kernel.process_cwd_ino(handle.vmPtr)).toBe(cwdIno);
    expect(handle.guestBase).toBeGreaterThan(handle.kernel.kernel_heap_ptr());
  } finally {
    runtime?.destroy();
  }
});
