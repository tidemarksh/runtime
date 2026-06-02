import { existsSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import type { SnapshotSource } from "../../../support/snapshot-sources.js";

const RUST_TARGET = "riscv64gc-unknown-linux-gnu";
const GCC_TRIPLE = "riscv64-linux-gnu";
const GCC_VERSION = "16";

const C_LINKER_BINARIES = [
  "ld",
  "riscv64-linux-gnu-gcc-16",
  "riscv64-linux-gnu-ld",
  "riscv64-linux-gnu-ld.bfd",
] as const;

const GCC_LINKER_FILES = [
  "crtbegin.o",
  "crtbeginS.o",
  "crtend.o",
  "crtendS.o",
  "crti.o",
  "crtn.o",
  "libatomic.a",
  "libgcc.a",
  "libgcc_eh.a",
  "libgcc_s.so",
  "liblto_plugin.so",
] as const;

const GCC_LIBEXEC_FILES = [
  "collect2",
  "liblto_plugin.so",
] as const;

const GLIBC_LINKER_FILES = [
  "Scrt1.o",
  "crt1.o",
  "crti.o",
  "crtn.o",
  "ld-linux-riscv64-lp64d.so.1",
  "libbfd-2.46-system.so",
  "libc.so",
  "libc.so.6",
  "libc_nonshared.a",
  "libctf.so.0",
  "libctf.so.0.0.0",
  "libdl.a",
  "libdl.so.2",
  "libgcc_s.so.1",
  "libjansson.so.4",
  "libjansson.so.4.14.0",
  "libm.so",
  "libm.so.6",
  "libpthread.a",
  "libpthread_nonshared.a",
  "libpthread.so.0",
  "librt.a",
  "librt.so.1",
  "libsframe.so.3",
  "libsframe.so.3.0.0",
  "libutil.a",
  "libutil.so.1",
  "libz.so.1",
  "libz.so.1.3.2",
  "libzstd.so.1",
  "libzstd.so.1.5.7",
] as const;

const RUST_HOST_LIB_PREFIXES = [
  "librustc_driver-",
] as const;

function missingFileSource(hostPath: string, guestPath: string): SnapshotSource {
  return { kind: "file", hostPath, guestPath };
}

function topLevelFileSources(hostDir: string, guestDir: string): SnapshotSource[] {
  if (!existsSync(hostDir)) {
    return [missingFileSource(resolve(hostDir, ".tidemark-required"), `${guestDir}/.tidemark-required`)];
  }

  return readdirSync(hostDir, { withFileTypes: true })
    .filter((entry) => entry.isFile() || entry.isSymbolicLink())
    .map((entry) => ({
      kind: "file" as const,
      hostPath: resolve(hostDir, entry.name),
      guestPath: `${guestDir}/${entry.name}`,
    }));
}

function topLevelFileSourcesByPrefix(
  hostDir: string,
  guestDir: string,
  prefixes: readonly string[],
): SnapshotSource[] {
  if (!existsSync(hostDir)) {
    return [missingFileSource(resolve(hostDir, ".tidemark-required"), `${guestDir}/.tidemark-required`)];
  }

  return readdirSync(hostDir, { withFileTypes: true })
    .filter((entry) => (
      (entry.isFile() || entry.isSymbolicLink())
      && prefixes.some((prefix) => entry.name.startsWith(prefix))
    ))
    .map((entry) => ({
      kind: "file" as const,
      hostPath: resolve(hostDir, entry.name),
      guestPath: `${guestDir}/${entry.name}`,
    }));
}

function fileSources(
  hostDir: string,
  guestDir: string,
  names: readonly string[],
  mode?: number,
): SnapshotSource[] {
  return names.map((name) => ({
    kind: "file" as const,
    hostPath: resolve(hostDir, name),
    guestPath: `${guestDir}/${name}`,
    mode,
  }));
}

export function rustToolchainSnapshotSources(rustToolchainDir: string): SnapshotSource[] {
  const rustlibDir = resolve(rustToolchainDir, "lib/rustlib");
  const targetRustlibDir = resolve(rustlibDir, RUST_TARGET);

  return [
    { kind: "dir", guestPath: "/toolchain" },
    { kind: "dir", guestPath: "/toolchain/bin" },
    { kind: "dir", guestPath: "/toolchain/lib" },
    { kind: "dir", guestPath: "/toolchain/lib/rustlib" },
    { kind: "dir", guestPath: `/toolchain/lib/rustlib/${RUST_TARGET}` },
    {
      kind: "file",
      hostPath: resolve(rustToolchainDir, "bin/rustc"),
      guestPath: "/toolchain/bin/rustc",
      mode: 0o755,
    },
    {
      kind: "file",
      hostPath: resolve(rustToolchainDir, "bin/cargo"),
      guestPath: "/toolchain/bin/cargo",
      mode: 0o755,
    },
    ...topLevelFileSourcesByPrefix(
      resolve(rustToolchainDir, "lib"),
      "/toolchain/lib",
      RUST_HOST_LIB_PREFIXES,
    ),
    ...topLevelFileSources(rustlibDir, "/toolchain/lib/rustlib"),
    {
      kind: "tree",
      hostPath: resolve(targetRustlibDir, "lib"),
      guestPath: `/toolchain/lib/rustlib/${RUST_TARGET}/lib`,
    },
  ];
}

export function rustCLinkerSnapshotSources(cToolchainDir: string): SnapshotSource[] {
  const gccLibDir = resolve(cToolchainDir, `usr/lib/gcc/${GCC_TRIPLE}/${GCC_VERSION}`);
  const gccLibexecDir = resolve(cToolchainDir, `usr/libexec/gcc/${GCC_TRIPLE}/${GCC_VERSION}`);
  const glibcLibDir = resolve(cToolchainDir, `usr/lib/${GCC_TRIPLE}`);

  return [
    { kind: "dir", guestPath: "/usr" },
    { kind: "dir", guestPath: "/usr/bin" },
    { kind: "dir", guestPath: "/usr/lib" },
    { kind: "dir", guestPath: "/usr/libexec" },
    { kind: "dir", guestPath: `/usr/lib/gcc` },
    { kind: "dir", guestPath: `/usr/lib/gcc/${GCC_TRIPLE}` },
    { kind: "dir", guestPath: `/usr/lib/gcc/${GCC_TRIPLE}/${GCC_VERSION}` },
    { kind: "dir", guestPath: `/usr/libexec/gcc` },
    { kind: "dir", guestPath: `/usr/libexec/gcc/${GCC_TRIPLE}` },
    { kind: "dir", guestPath: `/usr/libexec/gcc/${GCC_TRIPLE}/${GCC_VERSION}` },
    { kind: "dir", guestPath: `/usr/lib/${GCC_TRIPLE}` },
    ...fileSources(resolve(cToolchainDir, "usr/bin"), "/usr/bin", C_LINKER_BINARIES, 0o755),
    ...fileSources(
      gccLibDir,
      `/usr/lib/gcc/${GCC_TRIPLE}/${GCC_VERSION}`,
      GCC_LINKER_FILES,
    ),
    ...fileSources(
      gccLibexecDir,
      `/usr/libexec/gcc/${GCC_TRIPLE}/${GCC_VERSION}`,
      GCC_LIBEXEC_FILES,
    ),
    ...fileSources(glibcLibDir, `/usr/lib/${GCC_TRIPLE}`, GLIBC_LINKER_FILES),
  ];
}
