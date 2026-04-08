import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../support/dynamic-startup-harness.js";

const nodePath = process.env.TIDEMARK_TEST_NODE_BINARY ?? "";
const nodeSysrootDir = process.env.TIDEMARK_TEST_NODE_SYSROOT_DIR ?? "";

const NODE_RUNTIME_LIBS = [
  "ld-linux-riscv64-lp64d.so.1",
  "libc.so.6",
  "libgcc_s.so.1",
  "libm.so.6",
  "libstdc++.so.6",
  "libstdc++.so.6.0.35",
] as const;

export const nodeDynamicStartupEntry: DynamicStartupEntry = {
  id: "node-version",
  targetClass: "JavaScript runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: nodePath,
  guestExecutablePath: "/bin/node",
  argv: ["/bin/node", "--version"],
  env: [],
  timeoutMs: 5_000,
  ramSize: 256 * 1024 * 1024,
  snapshotSources: [
    ...NODE_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(nodeSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    { kind: "file", hostPath: nodePath, guestPath: "/bin/node", mode: 0o755 },
  ],
  expectedSubstrings: ["v24.14.1"],
};
