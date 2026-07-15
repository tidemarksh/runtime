import { resolve } from "node:path";
import type { PackageModuleDriverEntry } from "../../../support/package-module-driver-harness.js";
import { makeJavaRuntimeSnapshotSources } from "./runtime-snapshot.js";

const jdkDir = process.env.TIDEMARK_TEST_JAVA_RUNTIME_DIR ?? "";
const memory64HeadroomPages = (4 * 1024 * 1024 * 1024) / 65536 + 1;
const helloClassBytes = Uint8Array.from(Buffer.from(
  "yv66vgAAAEUAHQoAAgADBwAEDAAFAAYBABBqYXZhL2xhbmcvT2JqZWN0AQAGPGluaXQ+AQADKClWCQAIAAkHAAoMAAsADAEAEGphdmEvbGFuZy9TeXN0ZW0BAANvdXQBABVMamF2YS9pby9QcmludFN0cmVhbTsIAA4BABVqYXZhLWNvbXBpbGUtZXZpZGVuY2UKABAAEQcAEgwAEwAUAQATamF2YS9pby9QcmludFN0cmVhbQEAB3ByaW50bG4BABUoTGphdmEvbGFuZy9TdHJpbmc7KVYHABYBAAVIZWxsbwEABENvZGUBAA9MaW5lTnVtYmVyVGFibGUBAARtYWluAQAWKFtMamF2YS9sYW5nL1N0cmluZzspVgEAClNvdXJjZUZpbGUBAApIZWxsby5qYXZhACEAFQACAAAAAAACAAEABQAGAAEAFwAAAB0AAQABAAAABSq3AAGxAAAAAQAYAAAABgABAAAAAQAJABkAGgABABcAAAAlAAIAAQAAAAmyAAcSDbYAD7EAAAABABgAAAAKAAIAAAADAAgABAABABsAAAACABw=",
  "base64",
));

export const javaClassLoadPackageModuleEntry: PackageModuleDriverEntry = {
  id: "java-class-load",
  targetClass: "JVM runtime",
  workflowKind: "class loader",
  hostExecutablePath: resolve(jdkDir, "bin/java"),
  guestExecutablePath: "/jdk/bin/java",
  argv: ["/jdk/bin/java", "-Xshare:off", "-XX:CompressedClassSpaceSize=64m", "-cp", "/work", "Hello"],
  env: [],
  timeoutMs: 60_000,
  guestMemoryPolicyLimitBytes: memory64HeadroomPages * 65536,
  maxProcessMemoryPages: memory64HeadroomPages,
  maxForkMemoryPages: memory64HeadroomPages,
  snapshotSources: makeJavaRuntimeSnapshotSources([
    {
      kind: "inlineFile",
      data: helloClassBytes,
      guestPath: "/work/Hello.class",
      mode: 0o644,
    },
  ]),
  expectedDriverSubstrings: ["java-compile-evidence"],
};
