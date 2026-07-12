import { resolve } from "node:path";
import type { ScriptRuntimeEntry } from "../../../support/script-runtime-harness.js";

const busyboxBinary = process.env.TIDEMARK_TEST_BUSYBOX_BINARY ?? "";
const runtimeDir = process.env.TIDEMARK_TEST_R_RUNTIME_DIR ?? "";
const rhomeOverlayDir = process.env.TIDEMARK_TEST_R_HOME_OVERLAY_DIR ?? "";

function materializedRLibrary(hostPath: string, guestPath: string) {
  return {
    kind: "materializedFile" as const,
    hostPath: resolve(runtimeDir, hostPath),
    guestPath,
    mode: 0o755,
  };
}

function materializedRiscvLibrary(name: string, guestName = name) {
  return materializedRLibrary(
    `usr/lib/riscv64-linux-gnu/${name}`,
    `/usr/lib/riscv64-linux-gnu/${guestName}`,
  );
}

const materializedLoaderLibraries = [
  materializedRLibrary("usr/lib/libR.so", "/usr/lib/libR.so"),
  materializedRiscvLibrary("blas/libblas.so.3", "libblas.so.3"),
  materializedRiscvLibrary("lapack/liblapack.so.3", "liblapack.so.3"),
  materializedRiscvLibrary("libc.so.6"),
  materializedRiscvLibrary("libm.so.6"),
  materializedRiscvLibrary("libreadline.so.8"),
  materializedRiscvLibrary("libtinfo.so.6"),
  materializedRiscvLibrary("libpcre2-8.so.0"),
  materializedRiscvLibrary("libdeflate.so.0"),
  materializedRiscvLibrary("libzstd.so.1"),
  materializedRiscvLibrary("liblzma.so.5"),
  materializedRiscvLibrary("libbz2.so.1.0"),
  materializedRiscvLibrary("libz.so.1"),
  materializedRiscvLibrary("libtirpc.so.3"),
  materializedRiscvLibrary("libgssapi_krb5.so.2"),
  materializedRiscvLibrary("libkrb5.so.3"),
  materializedRiscvLibrary("libk5crypto.so.3"),
  materializedRiscvLibrary("libcom_err.so.2"),
  materializedRiscvLibrary("libkrb5support.so.0"),
  materializedRiscvLibrary("libkeyutils.so.1"),
  materializedRiscvLibrary("libresolv.so.2"),
  materializedRiscvLibrary("libicuuc.so.78"),
  materializedRiscvLibrary("libicui18n.so.78"),
  materializedRiscvLibrary("libicudata.so.78"),
  materializedRiscvLibrary("libstdc++.so.6"),
  materializedRiscvLibrary("libgomp.so.1"),
  materializedRiscvLibrary("libgfortran.so.5"),
  materializedRiscvLibrary("libgcc_s.so.1"),
  materializedRLibrary("usr/lib/ld-linux-riscv64-lp64d.so.1", "/lib/ld-linux-riscv64-lp64d.so.1"),
];

export const rScriptRuntimeEntry: ScriptRuntimeEntry = {
  id: "r-script",
  targetClass: "R runtime",
  runnerKind: "direct-exec",
  hostExecutablePath: resolve(runtimeDir, "usr/lib/R/bin/exec/R"),
  guestExecutablePath: "/usr/lib/R/bin/exec/R",
  argv: [
    "/usr/lib/R/bin/exec/R",
    "--slave",
    "-f",
    "/work/script.R",
  ],
  env: [
    "R_HOME=/usr/lib/R",
    "R_SHARE_DIR=/usr/share/R/share",
    "R_INCLUDE_DIR=/usr/share/R/include",
    "R_DOC_DIR=/usr/share/R/doc",
    "R_DEFAULT_PACKAGES=NULL",
  ],
  timeoutMs: 120_000,
  guestMemoryPolicyLimitBytes: 512 * 1024 * 1024,
  hostFileBackingMode: "host-file",
  snapshotSources: [
    {
      kind: "fileCopies",
      hostPath: busyboxBinary,
      guestPaths: ["/bin/busybox", "/bin/sh", "/bin/uname", "/bin/sed", "/bin/rm", "/usr/bin/which"],
      mode: 0o755,
    },
    {
      kind: "inlineFile",
      guestPath: "/bin/bash",
      data: '#!/bin/sh\nexec /bin/sh "$@"\n',
      mode: 0o755,
    },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/bin"), guestPath: "/usr/bin" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/R"), guestPath: "/usr/lib/R" },
    {
      kind: "file",
      hostPath: resolve(runtimeDir, "etc/R/ldpaths"),
      guestPath: "/usr/lib/R/etc/ldpaths",
      mode: 0o644,
    },
    {
      kind: "file",
      hostPath: resolve(rhomeOverlayDir, "etc/Renviron.ucf"),
      guestPath: "/usr/lib/R/etc/Renviron",
      mode: 0o644,
    },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/lib/riscv64-linux-gnu"), guestPath: "/usr/lib/riscv64-linux-gnu" },
    ...materializedLoaderLibraries,
    { kind: "tree", hostPath: resolve(runtimeDir, "etc"), guestPath: "/etc" },
    { kind: "tree", hostPath: resolve(runtimeDir, "usr/share/R"), guestPath: "/usr/share/R" },
    { kind: "tree", hostPath: rhomeOverlayDir, guestPath: "/rhome" },
    {
      kind: "file",
      hostPath: resolve(rhomeOverlayDir, "etc/Renviron.ucf"),
      guestPath: "/rhome/etc/Renviron",
      mode: 0o644,
    },
    {
      kind: "inlineFile",
      guestPath: "/work/script.R",
      data: 'cat("r-script-evidence\\n")\n',
    },
  ],
  expectedSubstrings: ["r-script-evidence"],
  forbiddenSubstrings: ["cannot find system Renviron"],
};
