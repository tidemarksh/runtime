import { expect, test } from "vite-plus/test";
import {
  findRiscvElf,
  readKernelWasm,
  Runtime,
  spawnWithOutput,
} from "../../support/runtime-workload.js";

for (const name of ["add", "addi", "beq", "jal", "ld_st", "simple"]) {
  test(`Runtime executes bundled riscv test ELF: ${name}`, async () => {
    const wasmBytes = readKernelWasm();
    const elfBytes = findRiscvElf(name);
    if (!wasmBytes || !elfBytes) {
      return;
    }

    const runtime = await Runtime.create({ wasmBytes });
    try {
      const { exitCode } = await spawnWithOutput(
        runtime,
        elfBytes,
        [name],
        [],
        5_000,
      );
      expect(exitCode).toBe(0);
    } finally {
      runtime.destroy();
    }
  }, 30_000);
}
