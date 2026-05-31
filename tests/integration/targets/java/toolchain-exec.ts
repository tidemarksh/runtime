import { resolve } from "node:path";
import type { ToolchainExecEntry } from "../../../support/toolchain-exec-harness.js";

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

export const javaJavacToolchainEntry: ToolchainExecEntry = {
  id: "java-javac",
  targetClass: "JVM runtime",
  workflowKind: "compiler driver",
  hostExecutablePath: resolve(jdkDir, "bin/javac"),
  guestExecutablePath: "/jdk/bin/javac",
  argv: ["/jdk/bin/javac", "-J-XX:CompressedClassSpaceSize=64m", "/work/Hello.java"],
  env: [],
  timeoutMs: 20_000,
  ramSize: 768 * 1024 * 1024,
  snapshotSources: [
    { kind: "tree", hostPath: jdkDir, guestPath: "/jdk" },
    ...JAVA_RUNTIME_LIBS.map((name) => ({
      kind: "file" as const,
      hostPath: resolve(glibcSysrootDir, name),
      guestPath: `/lib/${name}`,
      mode: 0o755,
    })),
    {
      kind: "inlineFile",
      guestPath: "/work/Hello.java",
      data: [
        "public final class Hello {",
        "  public static void main(String[] args) {",
        '    System.out.println("javac-evidence");',
        "  }",
        "}",
        "",
      ].join("\n"),
    },
  ],
  artifactChecks: [{ path: "/work/Hello.class" }],
};
