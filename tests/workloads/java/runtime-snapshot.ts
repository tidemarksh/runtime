import { resolve } from "node:path";
import type { SnapshotSource } from "../../support/snapshot-sources.js";

const jdkDir = process.env.TIDEMARK_TEST_JAVA_RUNTIME_DIR ?? "";
const glibcSysrootDir = process.env.TIDEMARK_TEST_GLIBC_SYSROOT_DIR ?? "";

const JAVA_RUNTIME_LIBS = [
  "ld-linux-riscv64-lp64d.so.1",
  "libc.so.6",
  "libdl.so.2",
  "libpthread.so.0",
  "libm.so.6",
  "librt.so.1",
  "libutil.so.1",
] as const;

export function makeJavaRuntimeSnapshotSources(
  extraSources: readonly SnapshotSource[] = [],
): SnapshotSource[] {
  return [
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
    ...JAVA_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    ...extraSources,
  ];
}
