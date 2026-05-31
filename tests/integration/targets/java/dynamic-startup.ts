import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../../support/dynamic-startup-harness.js";
import { makeJavaRuntimeSnapshotSources } from "./runtime-snapshot.js";

const jdkDir = process.env.TIDEMARK_TEST_JAVA_RUNTIME_DIR ?? "";

export const javaDynamicStartupEntry: DynamicStartupEntry = {
  id: "java-version",
  targetClass: "JVM runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(jdkDir, "bin/java"),
  guestExecutablePath: "/jdk/bin/java",
  argv: ["/jdk/bin/java", "-Xshare:off", "-XX:CompressedClassSpaceSize=64m", "-version"],
  env: [],
  timeoutMs: 20_000,
  ramSize: 1024 * 1024 * 1024,
  snapshotSources: makeJavaRuntimeSnapshotSources(),
  expectedSubstrings: ['openjdk version "25.0.2"'],
};
