import { resolve } from "node:path";
import type { PackageModuleDriverEntry } from "../../support/package-module-driver-harness.js";
import { makeJavaRuntimeSnapshotSources } from "./runtime-snapshot.js";

const jdkDir = process.env.TIDEMARK_TEST_JAVA_RUNTIME_DIR ?? "";

export const javaClassLoadPackageModuleEntry: PackageModuleDriverEntry = {
  id: "java-class-load",
  targetClass: "JVM runtime",
  workflowKind: "class loader",
  hostExecutablePath: resolve(jdkDir, "bin/java"),
  guestExecutablePath: "/jdk/bin/java",
  argv: ["/jdk/bin/java", "-Xshare:off", "-XX:CompressedClassSpaceSize=64m", "-cp", "/work", "Hello"],
  env: [],
  timeoutMs: 20_000,
  ramSize: 1024 * 1024 * 1024,
  snapshotSources: makeJavaRuntimeSnapshotSources([
    {
      kind: "file",
      hostPath: resolve(process.cwd(), "../artifacts/reference-linux/java/javac/Hello.class"),
      guestPath: "/work/Hello.class",
      mode: 0o644,
    },
  ]),
  expectedDriverSubstrings: ["java-compile-evidence"],
};
