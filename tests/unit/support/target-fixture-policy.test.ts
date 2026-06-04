import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, extname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vite-plus/test";

const here = dirname(fileURLToPath(import.meta.url));
const targetRoot = resolve(here, "../../integration/targets");

function listTypeScriptFiles(dir: string): string[] {
  const files: string[] = [];
  for (const entry of readdirSync(dir)) {
    const path = resolve(dir, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      files.push(...listTypeScriptFiles(path));
      continue;
    }
    if (stat.isFile() && extname(path) === ".ts") {
      files.push(path);
    }
  }
  return files;
}

test("target fixtures do not override resident page-cache capacity", () => {
  const offenders = listTypeScriptFiles(targetRoot)
    .filter((path) => readFileSync(path, "utf8").includes("pageCacheDataCapacityBytes"))
    .map((path) => relative(targetRoot, path));

  expect(offenders).toEqual([]);
});

test("c and cpp toolchain fixtures include required packaged compiler closure", () => {
  const requiredGuestPaths = ["/tmp", "/usr/include", "/usr/libexec"];
  for (const fixture of [
    "c/toolchain-exec.ts",
    "cpp/toolchain-exec.ts",
  ]) {
    const source = readFileSync(resolve(targetRoot, fixture), "utf8");
    for (const guestPath of requiredGuestPaths) {
      expect(source, `${fixture} should include ${guestPath}`).toContain(`guestPath: "${guestPath}"`);
    }
  }
});
