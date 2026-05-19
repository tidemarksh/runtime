import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
  spawnWithOutputOrDebug,
} from "../../support/runtime-workload.js";
import { buildSnapshotFromSources, type SnapshotSource } from "../../support/snapshot-sources.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";
const glibcSysroot = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";

function buildAllocatorBrkProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-glibc-allocator-brk-"));
  const sourcePath = join(tmp, "allocator_brk.c");
  const outputPath = join(tmp, "allocator-brk-probe");
  try {
    writeFileSync(sourcePath, [
      "#include <stdint.h>",
      "#include <stdio.h>",
      "#include <stdlib.h>",
      "#include <string.h>",
      "#include <unistd.h>",
      "",
      "static uint64_t checksum;",
      "",
      "static void touch(void *ptr, size_t size, unsigned seed) {",
      "  unsigned char *bytes = (unsigned char *)ptr;",
      "  for (size_t index = 0; index < size; index += 257) {",
      "    bytes[index] = (unsigned char)(seed + index);",
      "    checksum += bytes[index];",
      "  }",
      "}",
      "",
      "int main(void) {",
      "  void *held[96] = {0};",
      "  setvbuf(stdout, NULL, _IONBF, 0);",
      "  printf(\"allocator-brk-start brk=%p\\n\", sbrk(0));",
      "  for (unsigned iter = 0; iter < 12000; iter++) {",
      "    size_t size = 32 + ((iter * 131u) % 32768u);",
      "    void *ptr = malloc(size);",
      "    if (ptr == NULL) {",
      "      printf(\"allocator-brk-malloc-failed iter=%u size=%zu\\n\", iter, size);",
      "      return 12;",
      "    }",
      "    touch(ptr, size, iter);",
      "    if ((iter % 3u) == 0u) {",
      "      size_t grown = size + 4096u + ((iter * 17u) % 8192u);",
      "      void *next = realloc(ptr, grown);",
      "      if (next == NULL) {",
      "        free(ptr);",
      "        printf(\"allocator-brk-realloc-failed iter=%u size=%zu\\n\", iter, grown);",
      "        return 13;",
      "      }",
      "      ptr = next;",
      "      touch(ptr, grown, iter + 7u);",
      "    }",
      "    if ((iter % 97u) == 0u) {",
      "      unsigned slot = (iter / 97u) % 96u;",
      "      free(held[slot]);",
      "      held[slot] = ptr;",
      "    } else {",
      "      free(ptr);",
      "    }",
      "    if ((iter % 1000u) == 0u) {",
      "      printf(\"allocator-brk-progress iter=%u brk=%p checksum=%llu\\n\",",
      "        iter, sbrk(0), (unsigned long long)checksum);",
      "    }",
      "  }",
      "  for (unsigned index = 0; index < 96; index++) {",
      "    free(held[index]);",
      "  }",
      "  printf(\"allocator-brk-done brk=%p checksum=%llu\\n\",",
      "    sbrk(0), (unsigned long long)checksum);",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, [
      "-O2",
      "-Wl,--dynamic-linker=/lib/ld-linux-riscv64-lp64d.so.1",
      "-o",
      outputPath,
      sourcePath,
    ], { stdio: "pipe" });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build glibc allocator/brk probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildGlibcSnapshot() {
  const ldPath = resolve(glibcSysroot, "usr/lib/riscv64-linux-gnu/ld-linux-riscv64-lp64d.so.1");
  const libcPath = resolve(glibcSysroot, "usr/lib/riscv64-linux-gnu/libc.so.6");
  const snapshotSources: readonly SnapshotSource[] = [
    {
      kind: "fileCopies",
      hostPath: ldPath,
      guestPaths: [
        "/lib/ld-linux-riscv64-lp64d.so.1",
        "/usr/lib/ld-linux-riscv64-lp64d.so.1",
        "/usr/lib/riscv64-linux-gnu/ld-linux-riscv64-lp64d.so.1",
      ],
      mode: 0o755,
    },
    {
      kind: "fileCopies",
      hostPath: libcPath,
      guestPaths: [
        "/lib/riscv64-linux-gnu/libc.so.6",
        "/usr/lib/riscv64-linux-gnu/libc.so.6",
      ],
      mode: 0o755,
    },
  ];
  return buildSnapshotFromSources(snapshotSources, {
    missingLabel: "glibc allocator/brk sysroot",
  });
}

test("glibc allocator/brk local gate: dynamic ELF malloc/realloc/free progress", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  if (!glibcSysroot || !existsSync(glibcSysroot)) {
    return logSkip(`missing glibc sysroot: ${glibcSysroot || "set TIDEMARK_TEST_GLIBC_SYSROOT_DIR"}`);
  }
  const probeBytes = buildAllocatorBrkProbe();
  if (!probeBytes) {
    return;
  }
  const snapshot = buildGlibcSnapshot();
  if (snapshot.skipReason) {
    return logSkip(snapshot.skipReason);
  }

  const runtime = await Runtime.create({
    wasmBytes,
    ramSize: 128 * 1024 * 1024,
    maxProcessMemoryPages: 32768,
  });
  try {
    await runtime.applyFsSnapshot(snapshot.entries ?? []);
    const result = await spawnWithOutputOrDebug(
      runtime,
      probeBytes,
      ["/work/allocator-brk-probe"],
      ["LC_ALL=C", "LANG=C"],
      30_000,
      { stdin: "closed" },
    );
    expect(
      result.exitCode,
      [
        "glibc allocator/brk probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toContain("allocator-brk-start");
    expect(result.output).toContain("allocator-brk-progress iter=11000");
    expect(result.output).toContain("allocator-brk-done");
  } finally {
    runtime.destroy();
  }
}, 45_000);
