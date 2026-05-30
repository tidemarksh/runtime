import { readFileSync, readdirSync } from "node:fs";
import { join, relative } from "node:path";
import { expect, test } from "vite-plus/test";

const RUNTIME_SRC = join(process.cwd(), "src", "worker");

function tsFiles(root: string): string[] {
  const entries = readdirSync(root, { withFileTypes: true });
  return entries.flatMap((entry) => {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      return tsFiles(path);
    }
    return entry.isFile() && entry.name.endsWith(".ts") ? [path] : [];
  });
}

function relativeWorkerPath(path: string): string {
  return relative(RUNTIME_SRC, path).split("\\").join("/");
}

test("kernel-rpc and sync do not import each other directly", () => {
  const kernelRpcViolations = tsFiles(join(RUNTIME_SRC, "kernel-rpc"))
    .flatMap((path) => {
      const source = readFileSync(path, "utf8");
      const matches = [...source.matchAll(/from\s+["']([^"']+)["']/g)]
        .map((match) => match[1])
        .filter((specifier) => specifier === "../sync.js" || specifier.startsWith("../sync/"));
      return matches.map((specifier) => `${relativeWorkerPath(path)} -> ${specifier}`);
    });

  const syncViolations = tsFiles(join(RUNTIME_SRC, "sync"))
    .flatMap((path) => {
      const source = readFileSync(path, "utf8");
      const matches = [...source.matchAll(/from\s+["']([^"']+)["']/g)]
        .map((match) => match[1])
        .filter((specifier) => specifier === "../kernel-rpc.js" || specifier.startsWith("../kernel-rpc/"));
      return matches.map((specifier) => `${relativeWorkerPath(path)} -> ${specifier}`);
    });

  expect([...kernelRpcViolations, ...syncViolations]).toEqual([]);
});

