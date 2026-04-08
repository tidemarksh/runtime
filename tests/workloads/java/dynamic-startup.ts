import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../support/dynamic-startup-harness.js";

const jdkDir = process.env.TIDEMARK_TEST_JAVA_RUNTIME_DIR ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";

export const javaDynamicStartupEntry: DynamicStartupEntry = {
  id: "java-version",
  targetClass: "JVM runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(jdkDir, "bin/java"),
  guestExecutablePath: "/jdk/bin/java",
  argv: ["/jdk/bin/java", "-XX:CompressedClassSpaceSize=64m", "-version"],
  env: [],
  timeoutMs: 5_000,
  ramSize: 1024 * 1024 * 1024,
  snapshotSources: [
    { kind: "tree", hostPath: resolve(jdkDir, "bin"), guestPath: "/jdk/bin" },
    { kind: "file", hostPath: resolve(jdkDir, "lib/libjli.so"), guestPath: "/jdk/lib/libjli.so", mode: 0o755 },
    { kind: "file", hostPath: resolve(jdkDir, "lib/libjava.so"), guestPath: "/jdk/lib/libjava.so", mode: 0o755 },
    { kind: "file", hostPath: resolve(jdkDir, "lib/libverify.so"), guestPath: "/jdk/lib/libverify.so", mode: 0o755 },
    { kind: "file", hostPath: resolve(jdkDir, "lib/libzip.so"), guestPath: "/jdk/lib/libzip.so", mode: 0o755 },
    { kind: "file", hostPath: resolve(jdkDir, "lib/libjimage.so"), guestPath: "/jdk/lib/libjimage.so", mode: 0o755 },
    { kind: "file", hostPath: resolve(jdkDir, "lib/libnet.so"), guestPath: "/jdk/lib/libnet.so", mode: 0o755 },
    { kind: "file", hostPath: resolve(jdkDir, "lib/libnio.so"), guestPath: "/jdk/lib/libnio.so", mode: 0o755 },
    { kind: "file", hostPath: resolve(jdkDir, "lib/modules"), guestPath: "/jdk/lib/modules", mode: 0o644 },
    { kind: "file", hostPath: resolve(jdkDir, "lib/jvm.cfg"), guestPath: "/jdk/lib/jvm.cfg", mode: 0o644 },
    { kind: "tree", hostPath: resolve(jdkDir, "lib/server"), guestPath: "/jdk/lib/server" },
    { kind: "tree", hostPath: resolve(jdkDir, "conf"), guestPath: "/jdk/conf" },
    { kind: "file", hostPath: resolve(jdkDir, "release"), guestPath: "/jdk/release", mode: 0o644 },
    {
      kind: "file",
      hostPath: resolve(glibcSysrootDir, "ld-linux-riscv64-lp64d.so.1"),
      guestPath: "/lib/ld-linux-riscv64-lp64d.so.1",
      mode: 0o755,
    },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libc.so.6"), guestPath: "/lib/libc.so.6", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libdl.so.2"), guestPath: "/lib/libdl.so.2", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libpthread.so.0"), guestPath: "/lib/libpthread.so.0", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libm.so.6"), guestPath: "/lib/libm.so.6", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "librt.so.1"), guestPath: "/lib/librt.so.1", mode: 0o755 },
    { kind: "file", hostPath: resolve(glibcSysrootDir, "libutil.so.1"), guestPath: "/lib/libutil.so.1", mode: 0o755 },
  ],
  expectedSubstrings: ['openjdk version "25.0.2"'],
};
