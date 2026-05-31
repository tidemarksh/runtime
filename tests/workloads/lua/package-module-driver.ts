import { resolve } from "node:path";
import type { PackageModuleDriverEntry } from "../../support/package-module-driver-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_LUA_RUNTIME_DIR ?? "";

export const luaLuacPackageModuleEntry: PackageModuleDriverEntry = {
  id: "lua-luac-bytecode",
  targetClass: "Lua runtime",
  workflowKind: "bytecode driver",
  hostExecutablePath: resolve(runtimeDir, "usr/bin/luac5.4"),
  guestExecutablePath: "/usr/bin/luac5.4",
  argv: ["/usr/bin/luac5.4", "-o", "/work/out.luac", "/work/in.lua"],
  env: [],
  timeoutMs: 10_000,
  ramSize: 256 * 1024 * 1024,
  snapshotSources: [
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "usr/lib/ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
    {
      kind: "inlineFile",
      data: "return 6 * 7\n",
      guestPath: "/work/in.lua",
      mode: 0o644,
    },
  ],
  artifactChecks: [
    {
      path: "/work/out.luac",
      minSize: 16,
      expectedBinaryPrefix: [0x1b, 0x4c, 0x75, 0x61, 0x54],
    },
  ],
};
