import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../support/dynamic-startup-harness.js";

const runtimeDir = process.env.TIDEMARK_TEST_PYTHON_RUNTIME_DIR ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";

export const pythonDynamicStartupEntry: DynamicStartupEntry = {
  id: "python-version",
  targetClass: "Python runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "bin/python3.12"),
  guestExecutablePath: "/python/bin/python3.12",
  argv: ["/python/bin/python3.12", "--version"],
  env: [],
  timeoutMs: 5_000,
  ramSize: 256 * 1024 * 1024,
  snapshotSources: [
    { kind: "tree", hostPath: resolve(runtimeDir, "bin"), guestPath: "/python/bin" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "lib/libpython3.12.so.1.0"),
      guestPath: "/python/lib/libpython3.12.so.1.0",
      mode: 0o755,
    },
    {
      kind: "file",
      hostPath: resolve(glibcSysrootDir, "ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libc.so.6"), guestPath: "/lib/libc.so.6", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libm.so.6"), guestPath: "/lib/libm.so.6", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libpthread.so.0"), guestPath: "/lib/libpthread.so.0", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libdl.so.2"), guestPath: "/lib/libdl.so.2", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libutil.so.1"), guestPath: "/lib/libutil.so.1", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "librt.so.1"), guestPath: "/lib/librt.so.1", mode: 0o755 },
  ],
  expectedSubstrings: ["Python 3.12.13"],
};
