import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { relative, resolve, sep } from "node:path";

export interface ExecutionArtifact {
  name: string;
  bytes: Uint8Array;
}

export function executionArtifactDigest(artifacts: readonly ExecutionArtifact[]): string {
  if (artifacts.length === 0) {
    throw new Error("execution artifact set must not be empty");
  }
  const ordered = [...artifacts].sort((left, right) => (
    left.name < right.name ? -1 : left.name > right.name ? 1 : 0
  ));
  const hash = createHash("sha256");
  hash.update("execution-artifact-set-v1\0");
  let previous: string | undefined;
  for (const artifact of ordered) {
    if (!/^[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_.-]+)*$/.test(artifact.name)) {
      throw new Error(`invalid execution artifact name: ${JSON.stringify(artifact.name)}`);
    }
    if (previous === artifact.name) {
      throw new Error(`duplicate execution artifact name: ${JSON.stringify(artifact.name)}`);
    }
    previous = artifact.name;
    const name = new TextEncoder().encode(artifact.name);
    hash.update(lengthBytes(name.byteLength));
    hash.update(name);
    hash.update(lengthBytes(artifact.bytes.byteLength));
    hash.update(artifact.bytes);
  }
  return hash.digest("hex");
}

export function runtimeExecutionArtifactDigest(args: {
  distDirectory: string;
  kernelWasm: Uint8Array;
  probe: Uint8Array;
}): string {
  const runtimeFiles = listModuleFiles(args.distDirectory);
  for (const required of ["index.mjs", "kernel-worker.mjs", "thread-worker.mjs"]) {
    if (!runtimeFiles.includes(required)) {
      throw new Error(`missing Runtime execution artifact: ${required}`);
    }
  }
  return executionArtifactDigest([
    { name: "kernel/tidemark-kernel.wasm", bytes: args.kernelWasm },
    { name: "probe/execution-overhead", bytes: args.probe },
    ...runtimeFiles.map(path => ({
      name: `runtime/${path}`,
      bytes: new Uint8Array(readFileSync(resolve(args.distDirectory, path))),
    })),
  ]);
}

function listModuleFiles(directory: string): string[] {
  const files: string[] = [];
  const visit = (current: string): void => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const path = resolve(current, entry.name);
      if (entry.isDirectory()) {
        visit(path);
      } else if (entry.isFile() && entry.name.endsWith(".mjs")) {
        files.push(relative(directory, path).split(sep).join("/"));
      }
    }
  };
  visit(directory);
  return files.sort();
}

function lengthBytes(value: number): Uint8Array {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error("execution artifact length must be a non-negative safe integer");
  }
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value));
  return bytes;
}
