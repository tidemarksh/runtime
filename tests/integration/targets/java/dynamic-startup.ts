import { resolve } from "node:path";
import type { DynamicStartupEntry } from "../../../support/dynamic-startup-harness.js";
import { makeJavaRuntimeSnapshotSources } from "./runtime-snapshot.js";

const jdkDir = process.env.TIDEMARK_TEST_JAVA_RUNTIME_DIR ?? "";
const fourGiB = 4 * 1024 * 1024 * 1024;
const fourGiBPages = fourGiB / 65536;
const memory64HeadroomPages = fourGiBPages + 1;
const memory64HeadroomBytes = memory64HeadroomPages * 65536;

export const javaDynamicStartupEntry: DynamicStartupEntry = {
  id: "java-version",
  targetClass: "JVM runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(jdkDir, "bin/java"),
  guestExecutablePath: "/jdk/bin/java",
  argv: ["/jdk/bin/java", "-Xshare:off", "-XX:CompressedClassSpaceSize=64m", "-version"],
  env: [],
  timeoutMs: 60_000,
  guestMemoryPolicyLimitBytes: memory64HeadroomBytes,
  runtimeOptions: {
    maxProcessMemoryPages: memory64HeadroomPages,
    maxForkMemoryPages: memory64HeadroomPages,
  },
  snapshotSources: makeJavaRuntimeSnapshotSources(),
  expectedSubstrings: ['openjdk version "25.0.2"'],
};

export const javaFourGiBHeadroomEntry: DynamicStartupEntry = {
  ...javaDynamicStartupEntry,
  id: "java-version-memory-headroom-4g",
  guestMemoryPolicyLimitBytes: fourGiB,
  runtimeOptions: {
    ...javaDynamicStartupEntry.runtimeOptions,
    maxProcessMemoryPages: fourGiBPages,
    maxForkMemoryPages: fourGiBPages,
  },
};

export const javaMemory64HeadroomEntry: DynamicStartupEntry = {
  ...javaDynamicStartupEntry,
  id: "java-version-memory64-headroom",
  guestMemoryPolicyLimitBytes: memory64HeadroomBytes,
  runtimeOptions: {
    ...javaDynamicStartupEntry.runtimeOptions,
    maxProcessMemoryPages: memory64HeadroomPages,
    maxForkMemoryPages: memory64HeadroomPages,
  },
};
