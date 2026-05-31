import { readdirSync, statSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vite-plus/test";

const here = dirname(fileURLToPath(import.meta.url));
const testsRoot = resolve(here, "../..");
const unitRoot = resolve(testsRoot, "unit");
const workloadRoot = resolve(testsRoot, "workloads");
const targetRoot = resolve(testsRoot, "integration/targets");

type CanaryEvidence = {
  readonly workload: string;
  readonly contracts: readonly string[];
  readonly canaryReason: string;
  readonly primaryRuntimeEvidence: readonly string[];
};

const runtimeApiEvidence = [
  "api/kernel-wasm.test.ts",
  "api/process.test.ts",
  "api/spawn.test.ts",
  "thread-worker/session.test.ts",
  "thread-worker/exec.test.ts",
] as const;

const dynamicStartupEvidence = [
  "worker/process/create.test.ts",
  "worker/process/lifecycle.test.ts",
  "thread-worker/exec.test.ts",
  "execution/interpreter.test.ts",
] as const;

const execHandoffEvidence = [
  "worker/fork/execve-handoff.test.ts",
  "worker/fork/spawn-handoff.test.ts",
  "worker/fork/ownership-invariants.test.ts",
  "worker/process/create.test.ts",
  "worker/process/exit.test.ts",
  "thread-worker/exec.test.ts",
] as const;

const shellAndStdioEvidence = [
  "worker/io/stdio.test.ts",
  "worker/scheduler/liveness.test.ts",
  "worker/status/route.test.ts",
  "worker/process/child-processes.test.ts",
] as const;

const fsEvidence = [
  "fs/process-fs-kernel.test.ts",
  "fs/process-fs.test.ts",
  "fs/runtime-fs.test.ts",
  "kernel-worker/fs-state.test.ts",
  "worker/sync/memfs.test.ts",
] as const;

const largeFileEvidence = [
  "fs/process-fs-kernel.test.ts",
  "fs/process-fs.test.ts",
  "fs/runtime-fs.test.ts",
  "fs/sab-page-cache.test.ts",
  "thread-worker/memory.test.ts",
] as const;

const forkPipeEvidence = [
  "worker/fork/common.test.ts",
  "worker/fork/shared-ofd.test.ts",
  "worker/io/stdio.test.ts",
  "worker/scheduler/liveness.test.ts",
  "worker/status/route.test.ts",
  "kernel-worker/pipe.test.ts",
  "bridge/pipe.test.ts",
] as const;

const networkEvidence = [
  "worker/io/network-bridge.test.ts",
  "worker/status/route.test.ts",
  "worker/scheduler/liveness.test.ts",
  "kernel-worker/socket.test.ts",
  "kernel-worker/epoll.test.ts",
  "thread-worker/sync-effects.test.ts",
] as const;

const nodeEventLoopEvidence = [
  "thread-worker/blocking.test.ts",
  "thread-worker/sync-effects.test.ts",
  "worker/scheduler/liveness.test.ts",
  "kernel-worker/epoll.test.ts",
  "kernel-worker/eventfd.test.ts",
] as const;

const languageToolchainEvidence = [
  "worker/process/create.test.ts",
  "worker/process/exit.test.ts",
  "worker/exec-sequence.test.ts",
  "worker/io/stdio.test.ts",
  "worker/scheduler/liveness.test.ts",
  "fs/runtime-fs.test.ts",
] as const;

const packageModuleEvidence = [
  "worker/process/create.test.ts",
  "worker/fork/execve-handoff.test.ts",
  "worker/fork/spawn-handoff.test.ts",
  "worker/io/stdio.test.ts",
  "fs/runtime-fs.test.ts",
] as const;

const workloadCanaries: readonly CanaryEvidence[] = [
  {
    workload: "apt/method-http.test.ts",
    contracts: ["network bridge", "socket state sync", "scheduler liveness"],
    canaryReason: "APT method HTTP uses a package-manager style binary flow as a network canary; generic socket, bridge, and scheduler contracts are owned by runtime network tests.",
    primaryRuntimeEvidence: networkEvidence,
  },
  {
    workload: "archive-orchestrator.test.ts",
    contracts: ["process launch", "filesystem snapshot", "stdio drain"],
    canaryReason: "Archive orchestration validates an external archive workflow; generic process, filesystem, and stdio behavior is primary in owner runtime tests.",
    primaryRuntimeEvidence: [...dynamicStartupEvidence, ...fsEvidence, "worker/io/stdio.test.ts"],
  },
  {
    workload: "browser-direct-startup.test.ts",
    contracts: ["dynamic startup", "process creation", "interpreter execution"],
    canaryReason: "Browser-direct startup is a dynamic startup canary with external fixture shape; process creation and interpreter boundaries are owned by runtime tests.",
    primaryRuntimeEvidence: dynamicStartupEvidence,
  },
  {
    workload: "busybox/basic.test.ts",
    contracts: ["runtime spawn", "interactive stdin/stdout", "filesystem lookup", "scheduler liveness"],
    canaryReason: "Official BusyBox is a composite workload canary for spawn, PATH/env, interactive shell IO, filesystem listing, and shell liveness; each generic contract has owner runtime evidence.",
    primaryRuntimeEvidence: [...runtimeApiEvidence, ...shellAndStdioEvidence, ...fsEvidence],
  },
  {
    workload: "dynamic-startup-bucket.test.ts",
    contracts: ["dynamic startup", "process creation"],
    canaryReason: "Dynamic startup bucket covers fixture breadth; the generic startup and process creation contracts are primary in process and thread execution tests.",
    primaryRuntimeEvidence: dynamicStartupEvidence,
  },
  {
    workload: "dynamic-startup.test.ts",
    contracts: ["dynamic startup", "process creation"],
    canaryReason: "Dynamic startup is retained as workload coverage for representative binaries; generic startup contracts are owned by runtime process tests.",
    primaryRuntimeEvidence: dynamicStartupEvidence,
  },
  {
    workload: "execve-script.test.ts",
    contracts: ["execve handoff", "script interpreter path", "filesystem lookup"],
    canaryReason: "Host-built execve probes through BusyBox scripts are workload canaries; generic execve handoff and filesystem contracts are primary in fork/process/fs owner tests.",
    primaryRuntimeEvidence: [...execHandoffEvidence, ...fsEvidence, "worker/io/stdio.test.ts"],
  },
  {
    workload: "fork-memfs-page-cache.test.ts",
    contracts: ["fork shared state", "process memfs sync", "page cache"],
    canaryReason: "Fork plus memfs page-cache uses a full guest flow as a canary; shared state, memfs, and page-cache mechanics are owned by runtime tests.",
    primaryRuntimeEvidence: [...forkPipeEvidence, ...fsEvidence, "fs/sab-page-cache.test.ts"],
  },
  {
    workload: "fork-pipe-dup2.test.ts",
    contracts: ["fork", "pipe state", "dup2 stdio", "scheduler liveness"],
    canaryReason: "The fork/pipe/dup2 binary is a workload canary for combined process and pipe behavior; primary generic coverage is in fork, pipe, stdio, and scheduler tests.",
    primaryRuntimeEvidence: forkPipeEvidence,
  },
  {
    workload: "glibc/allocator-brk.test.ts",
    contracts: ["process memory", "thread prepare memory", "process launch"],
    canaryReason: "glibc allocator/brk behavior is a libc canary; runtime-owned memory snapshot, process creation, and execution tests cover the generic memory/process contracts.",
    primaryRuntimeEvidence: ["thread-worker/memory.test.ts", ...dynamicStartupEvidence],
  },
  {
    workload: "go/build.test.ts",
    contracts: ["toolchain process", "filesystem writes", "stdio"],
    canaryReason: "Go build is a language toolchain canary; Go compiler semantics are not a generic runtime contract, while process, fs, and stdio contracts are owner-tested.",
    primaryRuntimeEvidence: languageToolchainEvidence,
  },
  {
    workload: "go/capture.test.ts",
    contracts: ["toolchain process", "filesystem snapshot", "stdio"],
    canaryReason: "Go capture validates workload artifact handling; generic process, filesystem, and stdio behavior is primary in owner runtime tests.",
    primaryRuntimeEvidence: languageToolchainEvidence,
  },
  {
    workload: "go/driver-toolchain-exec.test.ts",
    contracts: ["toolchain exec", "process launch", "stdio"],
    canaryReason: "Go driver toolchain exec is a representative external toolchain canary; generic exec, process, and stdio contracts are runtime-owned.",
    primaryRuntimeEvidence: [...languageToolchainEvidence, ...execHandoffEvidence],
  },
  {
    workload: "go/driver-toolchain-input.test.ts",
    contracts: ["toolchain stdin", "process launch", "stdio"],
    canaryReason: "Go driver toolchain input stresses external compiler stdin handling; runtime-owned stdio and process tests are the primary generic evidence.",
    primaryRuntimeEvidence: languageToolchainEvidence,
  },
  {
    workload: "go/env.test.ts",
    contracts: ["environment propagation", "process launch"],
    canaryReason: "Go env is an external toolchain environment canary; generic argv/env and process setup contracts are covered in runtime process tests.",
    primaryRuntimeEvidence: ["worker/process/create.test.ts", "worker/process/lifecycle.test.ts", "api/spawn.test.ts"],
  },
  {
    workload: "go/helper-toolchain-exec.test.ts",
    contracts: ["toolchain exec", "process launch", "stdio"],
    canaryReason: "Go helper toolchain exec is a helper binary canary; generic exec/process/stdio behavior has runtime owner coverage.",
    primaryRuntimeEvidence: [...languageToolchainEvidence, ...execHandoffEvidence],
  },
  {
    workload: "go/toolchain.test.ts",
    contracts: ["toolchain process", "filesystem writes", "scheduler liveness"],
    canaryReason: "Go toolchain is retained for representative package-manager style workload breadth; generic runtime contracts are process, fs, stdio, and scheduler owner tests.",
    primaryRuntimeEvidence: languageToolchainEvidence,
  },
  {
    workload: "go/upstream-suite.test.ts",
    contracts: ["toolchain suite", "process scheduling", "filesystem writes"],
    canaryReason: "Go upstream suite validates external language behavior and is not primary evidence for generic runtime semantics; runtime contracts are owned by process/fs/stdio/scheduler tests.",
    primaryRuntimeEvidence: languageToolchainEvidence,
  },
  {
    workload: "java/dynamic-startup-contract.test.ts",
    contracts: ["dynamic startup", "process creation"],
    canaryReason: "Java dynamic startup is a language runtime canary; the generic dynamic startup and process contracts are covered by runtime owner tests.",
    primaryRuntimeEvidence: dynamicStartupEvidence,
  },
  {
    workload: "java/package-module-driver-isolation.test.ts",
    contracts: ["package module isolation", "process launch", "filesystem snapshot"],
    canaryReason: "Java package module isolation is workload-specific package behavior; generic process, exec, stdio, and fs contracts are runtime-owned.",
    primaryRuntimeEvidence: packageModuleEvidence,
  },
  {
    workload: "large-file-read.test.ts",
    contracts: ["large regular file read", "process fs", "thread memory"],
    canaryReason: "Large regular-file read is a size and artifact canary; generic file IO and memory snapshot behavior is primary in runtime fs and thread memory tests.",
    primaryRuntimeEvidence: largeFileEvidence,
  },
  {
    workload: "large-file-write.test.ts",
    contracts: ["large regular file write", "process fs", "thread memory"],
    canaryReason: "Large regular-file write is a size canary for representative artifacts; generic write and page-cache contracts are owner-tested in runtime fs tests.",
    primaryRuntimeEvidence: largeFileEvidence,
  },
  {
    workload: "large-sha256.test.ts",
    contracts: ["large file read", "pipe scheduling", "blocked sibling liveness"],
    canaryReason: "Large sha256 combines a package-index sized file with blocked sibling pressure; generic fs, pipe, and scheduler behavior is primary in runtime owner tests.",
    primaryRuntimeEvidence: [...largeFileEvidence, ...forkPipeEvidence],
  },
  {
    workload: "devtools/large-xz-decompress.test.ts",
    contracts: ["large file read", "large file write", "process memory"],
    canaryReason: "Large xz decompression is an external compression workload canary; generic large fs and process memory behavior is owner-tested.",
    primaryRuntimeEvidence: largeFileEvidence,
  },
  {
    workload: "libuv/upstream-suite.test.ts",
    contracts: ["event loop", "pipe/socket readiness", "scheduler liveness"],
    canaryReason: "libuv upstream coverage is external event-loop canary evidence; generic epoll, pipe, socket, and scheduler contracts are runtime-owned.",
    primaryRuntimeEvidence: [...nodeEventLoopEvidence, ...networkEvidence, ...forkPipeEvidence],
  },
  {
    workload: "network-fork-stream-file.test.ts",
    contracts: ["network bridge", "fork", "pipe stdio", "file streaming"],
    canaryReason: "Network fork stream file combines socket, fork, pipe, and file streaming in one workload; each generic contract has separate runtime owner evidence.",
    primaryRuntimeEvidence: [...networkEvidence, ...forkPipeEvidence, ...largeFileEvidence],
  },
  {
    workload: "network-large-response.test.ts",
    contracts: ["network bridge", "socket buffering", "scheduler liveness"],
    canaryReason: "Large network response is a network throughput canary; socket, bridge, epoll, and scheduler behavior is primary in runtime owner tests.",
    primaryRuntimeEvidence: networkEvidence,
  },
  {
    workload: "node/browser-direct-cli-startup.test.ts",
    contracts: ["dynamic startup", "process creation"],
    canaryReason: "Node browser-direct CLI startup is a Node fixture canary; dynamic startup and process creation are primary in runtime owner tests.",
    primaryRuntimeEvidence: dynamicStartupEvidence,
  },
  {
    workload: "node/cli-startup.test.ts",
    contracts: ["dynamic startup", "process creation"],
    canaryReason: "Node CLI startup is retained as a language runtime canary; generic startup/process contracts are owner-tested.",
    primaryRuntimeEvidence: dynamicStartupEvidence,
  },
  {
    workload: "node/event-loop.test.ts",
    contracts: ["event loop", "filesystem promises", "worker thread", "timer scheduling"],
    canaryReason: "Node event-loop fixtures combine Node timers, fs promises, and worker threads; generic blocking, epoll/eventfd, scheduler, and fs contracts are runtime-owned.",
    primaryRuntimeEvidence: [...nodeEventLoopEvidence, ...fsEvidence],
  },
  {
    workload: "node/execve-probe.test.ts",
    contracts: ["execve handoff", "fork exec", "dynamic loader"],
    canaryReason: "Node execve probes use a host-built guest binary and Node image as workload canaries; generic execve/fork handoff and process contracts are owner-tested.",
    primaryRuntimeEvidence: execHandoffEvidence,
  },
  {
    workload: "node/hello-debug.test.ts",
    contracts: ["debug output", "process launch", "stdio"],
    canaryReason: "Node hello debug is a workload debug canary; generic process output and diagnostic behavior is primary in runtime owner tests.",
    primaryRuntimeEvidence: ["worker/diagnostics/summary.test.ts", "worker/diagnostics/process.test.ts", "worker/io/stdio.test.ts", "worker/process/create.test.ts"],
  },
  {
    workload: "node/interactive-shell.test.ts",
    contracts: ["interactive stdin/stdout", "PATH lookup", "process launch", "filesystem snapshot"],
    canaryReason: "Node interactive shell is a demo-style shell canary with external Node and optional Go images; generic shell IO, process, and fs behavior is owner-tested.",
    primaryRuntimeEvidence: [...shellAndStdioEvidence, ...fsEvidence, "api/spawn.test.ts"],
  },
  {
    workload: "node/runtime.test.ts",
    contracts: ["process info", "os module", "process launch"],
    canaryReason: "Node runtime modules are language runtime canaries; generic process identity and launch behavior is primary in runtime process/API tests.",
    primaryRuntimeEvidence: ["worker/process/identity.test.ts", "worker/process/create.test.ts", "worker/process/lifecycle.test.ts", "api/process.test.ts"],
  },
  {
    workload: "node/smoke.test.ts",
    contracts: ["stdio", "timers", "filesystem read", "process launch"],
    canaryReason: "Node smoke tests representative language behavior; generic stdio, timer/blocking, fs, and process contracts are runtime-owned.",
    primaryRuntimeEvidence: ["worker/io/stdio.test.ts", "thread-worker/blocking.test.ts", "worker/scheduler/liveness.test.ts", ...fsEvidence, "worker/process/create.test.ts"],
  },
  {
    workload: "node/threading.test.ts",
    contracts: ["worker_threads", "futex/blocking", "scheduler liveness"],
    canaryReason: "Node worker_threads is a language threading canary; generic thread blocking, futex wait, sync effects, and scheduler behavior are primary in runtime owner tests.",
    primaryRuntimeEvidence: nodeEventLoopEvidence,
  },
  {
    workload: "package-module-driver.test.ts",
    contracts: ["package module isolation", "process launch", "filesystem snapshot"],
    canaryReason: "Package module driver is a package-layout canary; generic process, exec, stdio, and fs contracts are owner-tested.",
    primaryRuntimeEvidence: packageModuleEvidence,
  },
  {
    workload: "perl/package-module-driver-contract.test.ts",
    contracts: ["package module contract", "process launch", "filesystem snapshot"],
    canaryReason: "Perl package module contract is language/package behavior and not primary generic runtime evidence; runtime process/fs/stdio contracts are owner-tested.",
    primaryRuntimeEvidence: packageModuleEvidence,
  },
  {
    workload: "perl/package-module-driver-isolation.test.ts",
    contracts: ["package module isolation", "process launch", "filesystem snapshot"],
    canaryReason: "Perl package module isolation is a package workload canary; generic process, exec, stdio, and fs behavior is owner-tested.",
    primaryRuntimeEvidence: packageModuleEvidence,
  },
  {
    workload: "perl/script-runtime-isolation.test.ts",
    contracts: ["script runtime isolation", "process launch", "filesystem snapshot"],
    canaryReason: "Perl script runtime isolation validates language-specific script packaging; generic script/process/fs behavior is primary in runtime owner tests.",
    primaryRuntimeEvidence: packageModuleEvidence,
  },
  {
    workload: "process-create-dynamic.test.ts",
    contracts: ["process creation", "dynamic startup"],
    canaryReason: "Dynamic process-create is a workload canary for representative binaries; process creation and lifecycle contracts are runtime-owned.",
    primaryRuntimeEvidence: dynamicStartupEvidence,
  },
  {
    workload: "r/script-runtime-isolation.test.ts",
    contracts: ["script runtime isolation", "process launch", "filesystem snapshot"],
    canaryReason: "R script runtime isolation is language-specific workload evidence; generic process, exec, stdio, and fs contracts are owner-tested.",
    primaryRuntimeEvidence: packageModuleEvidence,
  },
  {
    workload: "riscv/bundled-elf.test.ts",
    contracts: ["bundled ELF spawn", "kernel wasm loading", "process API"],
    canaryReason: "Bundled RISC-V ELF is the minimal workload smoke canary; runtime API, kernel wasm loading, spawn, and thread execution are owner-tested.",
    primaryRuntimeEvidence: runtimeApiEvidence,
  },
  {
    workload: "ruby/dynamic-startup-contract.test.ts",
    contracts: ["dynamic startup", "process creation"],
    canaryReason: "Ruby dynamic startup is a language runtime canary; generic startup and process contracts are primary in runtime owner tests.",
    primaryRuntimeEvidence: dynamicStartupEvidence,
  },
  {
    workload: "ruby/package-module-driver-isolation.test.ts",
    contracts: ["package module isolation", "process launch", "filesystem snapshot"],
    canaryReason: "Ruby package module isolation validates language-specific packaging; generic process, exec, stdio, and fs behavior is owner-tested.",
    primaryRuntimeEvidence: packageModuleEvidence,
  },
  {
    workload: "ruby/script-runtime-isolation.test.ts",
    contracts: ["script runtime isolation", "process launch", "filesystem snapshot"],
    canaryReason: "Ruby script runtime isolation is workload-specific script packaging evidence; generic process, exec, stdio, and fs behavior is owner-tested.",
    primaryRuntimeEvidence: packageModuleEvidence,
  },
  {
    workload: "rust/upstream-suite.test.ts",
    contracts: ["threading", "pipe subprocess", "toolchain process"],
    canaryReason: "Rust upstream suite is external language suite evidence, including thread and pipe subprocess cases; generic thread, pipe, process, and stdio contracts are runtime-owned.",
    primaryRuntimeEvidence: [...nodeEventLoopEvidence, ...forkPipeEvidence, ...languageToolchainEvidence],
  },
  {
    workload: "script-runtime.test.ts",
    contracts: ["script runtime", "process launch", "filesystem snapshot"],
    canaryReason: "Script runtime is a cross-language workload canary; generic process, exec, stdio, and fs contracts are primary in runtime owner tests.",
    primaryRuntimeEvidence: packageModuleEvidence,
  },
  {
    workload: "shell-wrapper-exec.test.ts",
    contracts: ["shell wrapper exec", "stdio", "PATH/env", "process launch"],
    canaryReason: "BusyBox shell-wrapper exec chains are workload canaries; generic shell stdio, exec handoff, process, and fs contracts are owner-tested.",
    primaryRuntimeEvidence: [...execHandoffEvidence, ...shellAndStdioEvidence, ...fsEvidence],
  },
  {
    workload: "toolchain-exec.test.ts",
    contracts: ["toolchain exec", "process launch", "filesystem snapshot", "stdio"],
    canaryReason: "Toolchain exec is representative external toolchain canary evidence; generic process, exec, stdio, and filesystem contracts are runtime-owned.",
    primaryRuntimeEvidence: [...languageToolchainEvidence, ...execHandoffEvidence],
  },
];

function listTestFiles(root: string): string[] {
  const out: string[] = [];
  const visit = (dir: string) => {
    for (const name of readdirSync(dir).sort()) {
      const path = resolve(dir, name);
      if (statSync(path).isDirectory()) {
        visit(path);
      } else if (name.endsWith(".test.ts")) {
        out.push(relative(root, path).replaceAll("\\", "/"));
      }
    }
  };
  visit(root);
  return out;
}

function fileExists(root: string, path: string): boolean {
  return statSync(resolve(root, path), { throwIfNoEntry: false })?.isFile() ?? false;
}

describe("workload canary evidence", () => {
  test("every workload test has exact canary inventory and no stale inventory entries", () => {
    const workloadTests = [
      ...listTestFiles(workloadRoot),
      ...listTestFiles(targetRoot),
    ].sort();
    const inventory = workloadCanaries.map(({ workload }) => workload).sort();
    const duplicateInventory = inventory.filter((path, index) => inventory.indexOf(path) !== index);

    expect(duplicateInventory).toEqual([]);
    expect(inventory).toEqual(workloadTests);
  });

  test("each workload canary records the generic contracts and owner evidence", () => {
    const invalidEntries = workloadCanaries.flatMap((entry) => {
      const failures: string[] = [];
      if (entry.contracts.length === 0) {
        failures.push(`${entry.workload}: missing contracts`);
      }
      if (entry.canaryReason.length < 80) {
        failures.push(`${entry.workload}: canary reason is too terse`);
      }
      if (entry.primaryRuntimeEvidence.length === 0) {
        failures.push(`${entry.workload}: missing primary runtime evidence`);
      }
      for (const evidencePath of entry.primaryRuntimeEvidence) {
        if (evidencePath.startsWith("support/")) {
          failures.push(`${entry.workload}: support test cannot be primary evidence: ${evidencePath}`);
        }
        if (!fileExists(unitRoot, evidencePath)) {
          failures.push(`${entry.workload}: missing primary runtime evidence: ${evidencePath}`);
        }
      }
      return failures;
    });

    expect(invalidEntries).toEqual([]);
  });
});
