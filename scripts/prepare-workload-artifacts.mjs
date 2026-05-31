#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, isAbsolute, resolve } from "node:path";

const DEFAULT_REPOSITORY = "https://github.com/tidemarksh/artifacts";
const DEFAULT_ASSET = "payload.tar.zst";
const DEFAULT_OUTPUT = ".artifacts/workload-env.json";
const GITHUB_TOKEN = process.env.GH_TOKEN ?? process.env.GITHUB_TOKEN ?? "";

function usage() {
  return [
    "usage: node scripts/prepare-workload-artifacts.mjs --manifest <path> [--profile <name>] [--cache-dir <path>] [--output <path>]",
    "",
    "The manifest format matches artifacts/manifests/runtime-workloads.example.json.",
    "Each selected release asset is downloaded, SHA256-verified, extracted, and mapped",
    "onto the existing TIDEMARK_TEST_* workload environment variables.",
  ].join("\n");
}

function parseArgs(argv) {
  const args = {
    manifest: process.env.TIDEMARK_TEST_ARTIFACT_MANIFEST ?? "",
    cacheDir: process.env.TIDEMARK_TEST_ARTIFACT_CACHE_DIR ?? ".artifacts/cache",
    output: process.env.TIDEMARK_TEST_WORKLOAD_ARTIFACT_ENV ?? DEFAULT_OUTPUT,
    profiles: [],
    clean: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--") {
      continue;
    } else if (arg === "--manifest") {
      args.manifest = argv[++index] ?? "";
    } else if (arg === "--profile") {
      args.profiles.push(argv[++index] ?? "");
    } else if (arg === "--cache-dir") {
      args.cacheDir = argv[++index] ?? "";
    } else if (arg === "--output") {
      args.output = argv[++index] ?? "";
    } else if (arg === "--clean") {
      args.clean = true;
    } else if (arg === "--help" || arg === "-h") {
      console.log(usage());
      process.exit(0);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }
  if (!args.manifest) {
    throw new Error("missing --manifest");
  }
  return args;
}

function absolute(path) {
  return isAbsolute(path) ? path : resolve(process.cwd(), path);
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function writeJson(path, value) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function sha256File(path) {
  const hash = createHash("sha256");
  hash.update(readFileSync(path));
  return hash.digest("hex");
}

function authHeaders(extra = {}) {
  return GITHUB_TOKEN
    ? { ...extra, Authorization: `Bearer ${GITHUB_TOKEN}`, "User-Agent": "tidemark-runtime-artifacts" }
    : { ...extra, "User-Agent": "tidemark-runtime-artifacts" };
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: authHeaders({ Accept: "application/vnd.github+json" }),
  });
  if (!response.ok) {
    throw new Error(`failed to fetch ${url}: ${response.status}`);
  }
  return await response.json();
}

function githubRepositorySlug(repository) {
  const match = /^https:\/\/github\.com\/([^/]+\/[^/]+)\/?$/.exec(repository);
  return match?.[1] ?? null;
}

async function githubReleaseAssetUrl(repository, releaseTag, asset) {
  const slug = githubRepositorySlug(repository);
  if (!slug || !GITHUB_TOKEN) {
    return null;
  }
  const release = await fetchJson(
    `https://api.github.com/repos/${slug}/releases/tags/${encodeURIComponent(releaseTag)}`,
  );
  const matched = release.assets?.find((item) => item.name === asset);
  if (!matched?.url) {
    throw new Error(`release asset not found: ${repository} ${releaseTag} ${asset}`);
  }
  return matched.url;
}

async function downloadFile(url, path) {
  if (existsSync(path)) {
    return;
  }
  if (url.startsWith("file://")) {
    const sourcePath = new URL(url);
    mkdirSync(dirname(path), { recursive: true });
    copyFileSync(sourcePath, path);
    return;
  }
  const response = await fetch(url, { headers: authHeaders() });
  if (!response.ok) {
    throw new Error(`failed to download ${url}: ${response.status}`);
  }
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, Buffer.from(await response.arrayBuffer()));
}

async function downloadGitHubAsset(url, path) {
  if (existsSync(path)) {
    return;
  }
  const response = await fetch(url, {
    headers: authHeaders({ Accept: "application/octet-stream" }),
  });
  if (!response.ok) {
    throw new Error(`failed to download ${url}: ${response.status}`);
  }
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, Buffer.from(await response.arrayBuffer()));
}

async function downloadVerified({ url, path, sha256 }) {
  if (!sha256 || sha256 === "TODO") {
    throw new Error(`missing pinned sha256 for ${url}`);
  }
  await downloadFile(url, path);
  verifyFileSha256(path, sha256, url);
}

function verifyFileSha256(path, sha256, label) {
  const actual = sha256File(path);
  if (actual !== sha256) {
    rmSync(path, { force: true });
    throw new Error(`sha256 mismatch for ${label}: expected ${sha256}, got ${actual}`);
  }
}

async function downloadPinnedJson({ url, path, sha256 }) {
  if (url.startsWith("https://api.github.com/")) {
    await downloadGitHubAsset(url, path);
    verifyFileSha256(path, sha256, url);
  } else {
    await downloadVerified({ url, path, sha256 });
  }
  return readJson(path);
}

function releaseUrl(repository, releaseTag, asset) {
  return `${repository.replace(/\/$/, "")}/releases/download/${encodeURIComponent(releaseTag)}/${asset}`;
}

function extractTarZst(archivePath, outDir) {
  rmSync(outDir, { recursive: true, force: true });
  mkdirSync(outDir, { recursive: true });
  execFileSync("tar", ["--zstd", "-xf", archivePath, "-C", outDir], { stdio: "inherit" });
}

function uniqueArtifacts(manifest, profiles) {
  const selectedProfiles = profiles.length > 0
    ? profiles
    : manifest.defaultProfiles ?? Object.keys(manifest.profiles ?? {});
  const byKey = new Map();
  for (const profile of selectedProfiles) {
    const entries = manifest.profiles?.[profile];
    if (!Array.isArray(entries)) {
      throw new Error(`unknown artifact profile: ${profile}`);
    }
    for (const entry of entries) {
      const asset = entry.asset ?? DEFAULT_ASSET;
      byKey.set(`${entry.releaseTag}:${asset}`, { ...entry, asset });
    }
  }
  return [...byKey.values()];
}

function firstExisting(root, paths) {
  for (const path of paths) {
    const candidate = resolve(root, path);
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}

function setEnv(env, name, value) {
  if (value) {
    env[name] = value;
  }
}

function writeAptMethodPayload(path) {
  writeFileSync(path, `${[
    "601 Configuration",
    "Config-Item: Acquire::http::Proxy=http://127.0.0.1:8080",
    "Config-Item: Acquire::http::Timeout=10",
    "Config-Item: APT::Architecture=riscv64",
    "",
    "600 URI Acquire",
    "URI: http://deb.debian.org/debian/dists/trixie/InRelease",
    "Filename: /var/lib/apt/lists/partial/deb.debian.org_debian_dists_trixie_InRelease",
    "",
  ].join("\n")}\n`);
}

function linkOrCopy(from, to) {
  mkdirSync(dirname(to), { recursive: true });
  rmSync(to, { force: true });
  try {
    symlinkSync(from, to);
  } catch {
    copyFileSync(from, to);
  }
}

function makeFlatNodeSysroot(root, adapterDir) {
  const libs = [
    "ld-linux-riscv64-lp64d.so.1",
    "libc.so.6",
    "libgcc_s.so.1",
    "libm.so.6",
    "libstdc++.so.6",
    "libstdc++.so.6.0.35",
  ];
  const searchDirs = [
    root,
    resolve(root, "lib"),
    resolve(root, "lib/riscv64-linux-gnu"),
    resolve(root, "usr/lib"),
    resolve(root, "usr/lib/riscv64-linux-gnu"),
  ];
  rmSync(adapterDir, { recursive: true, force: true });
  mkdirSync(adapterDir, { recursive: true });
  for (const lib of libs) {
    const source = searchDirs.map((dir) => resolve(dir, lib)).find(existsSync);
    if (source) {
      linkOrCopy(source, resolve(adapterDir, lib));
    }
  }
  return adapterDir;
}

function applyExplicitEnv(env, root, explicitEnv) {
  for (const [name, value] of Object.entries(explicitEnv ?? {})) {
    if (Array.isArray(value)) {
      const paths = value.map((item) => resolve(root, item)).filter(existsSync);
      setEnv(env, name, paths.join(":"));
    } else if (typeof value === "string") {
      setEnv(env, name, resolve(root, value));
    } else {
      throw new Error(`invalid env mapping for ${name}`);
    }
  }
}

function applyKnownEnv(env, artifact, root, adapterRoot) {
  const id = artifact.id ?? artifact.artifactId ?? "";
  if (id.startsWith("busybox-riscv64")) {
    setEnv(env, "TIDEMARK_TEST_BUSYBOX_BINARY", firstExisting(root, [
      "bin/busybox",
      "usr/bin/busybox",
      "busybox",
    ]));
  }
  if (id.startsWith("go-riscv64")) {
    const goBinary = firstExisting(root, ["usr/local/go/bin/go", "bin/go"]);
    const goRoot = goBinary ? dirname(goBinary) : null;
    const toolchainDir = goRoot?.endsWith("/bin")
      ? dirname(goRoot)
      : (goRoot ?? firstExisting(root, ["usr/local/go", "."]));
    setEnv(env, "TIDEMARK_TEST_GO_TOOLCHAIN_DIR", toolchainDir);
    setEnv(env, "TIDEMARK_TEST_GO_BINARY", toolchainDir ? resolve(toolchainDir, "bin/go") : null);
  }
  if (id.startsWith("go-exec-capture-helper-riscv64")) {
    setEnv(env, "TIDEMARK_TEST_GO_EXEC_CAPTURE_HELPER", firstExisting(root, [
      "opt/tidemark/go-exec-capture-helper/exec-capture-helper.riscv64",
      "bin/exec-capture-helper.riscv64",
      "exec-capture-helper.riscv64",
    ]));
  }
  if (id === "node-riscv64-debian-trixie" || id.startsWith("node-riscv64-")) {
    setEnv(env, "TIDEMARK_TEST_NODE_BINARY", firstExisting(root, [
      "usr/bin/node",
      "usr/bin/nodejs",
      "bin/node",
    ]));
  }
  if (id === "node-sysroot-riscv64-debian-trixie" || id.startsWith("node-sysroot-riscv64-")) {
    setEnv(env, "TIDEMARK_TEST_NODE_SYSROOT_DIR", makeFlatNodeSysroot(root, resolve(adapterRoot, id)));
  }
  if (id === "apt-riscv64-debian-trixie" || id.startsWith("apt-riscv64-")) {
    const aptMethodPayloadPath = resolve(adapterRoot, id, "apt-method-payload.txt");
    mkdirSync(dirname(aptMethodPayloadPath), { recursive: true });
    writeAptMethodPayload(aptMethodPayloadPath);
    setEnv(env, "TIDEMARK_TEST_APT_METHOD_ROOT", root);
    setEnv(env, "TIDEMARK_TEST_APT_METHOD_LIBRARY_ROOTS", root);
    setEnv(env, "TIDEMARK_TEST_APT_METHOD_PAYLOAD", aptMethodPayloadPath);
    setEnv(env, "TIDEMARK_TEST_APT_GET_ROOT", root);
    setEnv(env, "TIDEMARK_TEST_APT_SQV_ROOT", root);
  }
  if (id.startsWith("glibc-riscv64")) {
    setEnv(env, "TIDEMARK_TEST_GLIBC_SYSROOT_DIR", root);
  }
  if (id.startsWith("rust-riscv64")) {
    setEnv(env, "TIDEMARK_TEST_RUST_TOOLCHAIN_DIR", root);
  }
  if (id.startsWith("c-toolchain-riscv64")) {
    setEnv(env, "TIDEMARK_TEST_C_TOOLCHAIN_DIR", root);
  }
  if (id.startsWith("cpp-toolchain-riscv64")) {
    setEnv(env, "TIDEMARK_TEST_CPP_TOOLCHAIN_DIR", root);
  }
  if (id.startsWith("devtools-riscv64")) {
    setEnv(env, "TIDEMARK_TEST_DEVTOOLS_TOOLSET_DIR", root);
  }
  if (id.startsWith("libuv-suite-")) {
    setEnv(env, "TIDEMARK_TEST_LIBUV_UPSTREAM_SUITE_DIR", root);
  }
  if (id.startsWith("java-")) {
    setEnv(env, "TIDEMARK_TEST_JAVA_RUNTIME_DIR", root);
  }
  if (id.startsWith("python-")) {
    setEnv(env, "TIDEMARK_TEST_PYTHON_RUNTIME_DIR", root);
  }
  if (id.startsWith("ruby-")) {
    setEnv(env, "TIDEMARK_TEST_RUBY_RUNTIME_DIR", root);
  }
  if (id.startsWith("perl-")) {
    setEnv(env, "TIDEMARK_TEST_PERL_RUNTIME_DIR", root);
  }
  if (id.startsWith("php-")) {
    setEnv(env, "TIDEMARK_TEST_PHP_RUNTIME_DIR", root);
  }
  if (id.startsWith("lua-")) {
    setEnv(env, "TIDEMARK_TEST_LUA_RUNTIME_DIR", root);
  }
  if (id.startsWith("r-riscv64")) {
    setEnv(env, "TIDEMARK_TEST_R_RUNTIME_DIR", root);
  }
  if (id.startsWith("elixir-erlang-")) {
    setEnv(env, "TIDEMARK_TEST_ELIXIR_RUNTIME_DIR", root);
  }
  if (id.startsWith("haskell-")) {
    setEnv(env, "TIDEMARK_TEST_HASKELL_TOOLCHAIN_DIR", root);
  }
}

async function applySourceManifestEnv(env, artifact, repository, cacheDir) {
  const id = artifact.id ?? artifact.artifactId ?? "";
  if (!(id === "apt-riscv64-debian-trixie" || id.startsWith("apt-riscv64-"))) {
    return;
  }
  let packageIndex = artifact.packageIndex;
  if (!packageIndex && artifact.sourceManifestSha256) {
  const sourceManifestUrl = await githubReleaseAssetUrl(
    repository,
    artifact.releaseTag,
    "source-manifest.json",
  ) ?? releaseUrl(repository, artifact.releaseTag, "source-manifest.json");
    const sourceManifestPath = resolve(cacheDir, "sources", artifact.releaseTag, "source-manifest.json");
    const sourceManifest = await downloadPinnedJson({
      url: sourceManifestUrl,
      path: sourceManifestPath,
      sha256: artifact.sourceManifestSha256,
    });
    packageIndex = sourceManifest.packageIndexes?.find((item) => item.url?.endsWith("/Packages.xz"));
  }
  if (!packageIndex) {
    return;
  }
  const packageIndexPath = resolve(
    cacheDir,
    "sources",
    artifact.releaseTag,
    packageIndex.fileName ?? basename(packageIndex.url),
  );
  await downloadVerified({
    url: packageIndex.url,
    path: packageIndexPath,
    sha256: packageIndex.sha256,
  });
  setEnv(env, "TIDEMARK_TEST_APT_METHOD_PACKAGES_XZ_RESPONSE_BODY", packageIndexPath);
  setEnv(env, "TIDEMARK_TEST_APT_CACHE_PACKAGES_INPUT", packageIndexPath);
  setEnv(env, "TIDEMARK_TEST_APT_CHECK_PACKAGES_INPUT", packageIndexPath);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const manifestPath = absolute(args.manifest);
  const cacheDir = absolute(args.cacheDir);
  const outputPath = absolute(args.output);
  const manifest = readJson(manifestPath);
  const repository = manifest.repository ?? DEFAULT_REPOSITORY;
  const artifacts = uniqueArtifacts(manifest, args.profiles);
  const env = {};
  const extracted = [];

  if (args.clean) {
    rmSync(cacheDir, { recursive: true, force: true });
  }
  mkdirSync(cacheDir, { recursive: true });

  for (const artifact of artifacts) {
    const artifactRepository = artifact.repository ?? repository;
    const asset = artifact.asset ?? DEFAULT_ASSET;
    const assetApiUrl = artifact.url
      ? null
      : await githubReleaseAssetUrl(artifactRepository, artifact.releaseTag, asset);
    const url = artifact.url ?? assetApiUrl ?? releaseUrl(artifactRepository, artifact.releaseTag, asset);
    const archivePath = resolve(cacheDir, "downloads", artifact.releaseTag, asset);
    if (assetApiUrl) {
      await downloadGitHubAsset(assetApiUrl, archivePath);
      verifyFileSha256(archivePath, artifact.sha256, assetApiUrl);
    } else {
      await downloadVerified({ url, path: archivePath, sha256: artifact.sha256 });
    }

    const digest = artifact.sha256.slice(0, 16);
    const extractDir = resolve(cacheDir, "payloads", `${artifact.id}-${digest}`);
    if (!existsSync(resolve(extractDir, ".tidemark-extracted"))) {
      extractTarZst(archivePath, extractDir);
      writeFileSync(resolve(extractDir, ".tidemark-extracted"), `${artifact.sha256}\n`);
    }
    const adapterRoot = resolve(cacheDir, "adapters");
    applyKnownEnv(env, artifact, extractDir, adapterRoot);
    applyExplicitEnv(env, extractDir, artifact.env);
    await applySourceManifestEnv(env, artifact, artifactRepository, cacheDir);
    extracted.push({
      id: artifact.id,
      releaseTag: artifact.releaseTag,
      asset,
      sha256: artifact.sha256,
      payloadDir: extractDir,
    });
  }

  writeJson(outputPath, {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    manifest: manifestPath,
    cacheDir,
    env,
    artifacts: extracted,
  });
  console.log(`wrote workload artifact env: ${outputPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
