import { expect, test } from "vite-plus/test";
import {
  preparePackageModuleDriverRuntime,
  runPackageModuleDriverEntry,
  validatePackageModuleDriverOutput,
} from "../../support/package-module-driver-harness.js";
import { perlProvePackageModuleEntry } from "./package-module-driver.js";

interface PerlPackageDriverContractCase {
  id: string;
  argv: readonly string[];
  expectedExitCode: number;
  expectedDriverSubstrings?: readonly string[];
  expectedDebugSubstring?: string;
  expectedPathSubstring?: string;
  expectedFaultVmaSubstring?: string;
  expectHeapJumpAfterBrk?: boolean;
}

function parseDebugHexField(debugInfo: string, field: string): number | null {
  const match = debugInfo.match(new RegExp(`${field}=0x([0-9a-f]+)`, "i"));
  return match ? Number.parseInt(match[1], 16) : null;
}

function parseDebugIntField(debugInfo: string, field: string): number | null {
  const match = debugInfo.match(new RegExp(`${field}=(-?\\d+)`, "i"));
  return match ? Number.parseInt(match[1], 10) : null;
}

const perlContractCases: readonly PerlPackageDriverContractCase[] = [
  {
    id: "perl-package-driver-lower-bound-contract",
    argv: ["/usr/bin/perl", "-e", 'print "perl-ok\\n";'],
    expectedExitCode: 0,
    expectedDriverSubstrings: ["perl-ok"],
  },
  {
    id: "perl-pure-perl-module-load-contract",
    argv: ["/usr/bin/perl", "-Mstrict", "-Mwarnings", "-e", 'print "pure-module-ok\\n";'],
    expectedExitCode: 0,
    expectedDriverSubstrings: ["pure-module-ok"],
  },
  {
    id: "perl-xs-list-util-module-load-contract",
    argv: ["/usr/bin/perl", "-MList::Util=first", "-e", 'print "list-util-ok\\n";'],
    expectedExitCode: 0,
    expectedDriverSubstrings: ["list-util-ok"],
  },
  {
    id: "perl-xs-cwd-module-load-contract",
    argv: ["/usr/bin/perl", "-MCwd", "-e", 'print "cwd-ok\\n";'],
    expectedExitCode: 0,
    expectedDriverSubstrings: ["cwd-ok"],
  },
  {
    id: "perl-json-pp-module-load-contract",
    argv: [
      "/usr/bin/perl",
      "-MJSON::PP",
      "-e",
      'print JSON::PP->new->canonical->encode({ lang => "perl", n => 42 }), qq(\\n);',
    ],
    expectedExitCode: 0,
    expectedDriverSubstrings: ['{"lang":"perl","n":42}'],
  },
  {
    id: "perl-app-prove-module-load-contract",
    argv: ["/usr/bin/perl", "-MApp::Prove", "-e", 'print "app-prove-ok\\n";'],
    expectedExitCode: 0,
    expectedDriverSubstrings: ["app-prove-ok"],
  },
] as const;

for (const contractCase of perlContractCases) {
  test(`perl package/module driver contract: ${contractCase.id}`, async () => {
    const entry = {
      ...perlProvePackageModuleEntry,
      id: contractCase.id,
      argv: contractCase.argv,
      expectedDriverSubstrings: contractCase.expectedDriverSubstrings ?? [],
    };

    const prepared = await preparePackageModuleDriverRuntime(entry);
    if (!prepared) {
      return;
    }

    const { runtime, executableBytes } = prepared;
    try {
      const result = await runPackageModuleDriverEntry(runtime, entry, executableBytes);
      const mismatches = validatePackageModuleDriverOutput(entry, result.output);

      expect(
        result.exitCode,
        [
          `perl package/module driver contract exit mismatch: ${contractCase.id}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(contractCase.expectedExitCode);

      if (contractCase.expectedExitCode === 0) {
        expect(
          mismatches,
          [
            `perl package/module driver contract output mismatch: ${contractCase.id}`,
            `output=${JSON.stringify(result.output)}`,
            `debug=${result.debugInfo ?? ""}`,
          ].join("\n"),
        ).toEqual([]);
      } else {
        expect(
          result.debugInfo ?? "",
          [
            `perl package/module driver contract debug mismatch: ${contractCase.id}`,
            `output=${JSON.stringify(result.output)}`,
            `debug=${result.debugInfo ?? ""}`,
          ].join("\n"),
        ).toContain(contractCase.expectedDebugSubstring ?? "");

        if (contractCase.expectedPathSubstring) {
          expect(
            result.debugInfo ?? "",
            [
              `perl package/module driver contract path mismatch: ${contractCase.id}`,
              `output=${JSON.stringify(result.output)}`,
              `debug=${result.debugInfo ?? ""}`,
            ].join("\n"),
          ).toContain(contractCase.expectedPathSubstring);
        }

        if (contractCase.expectedFaultVmaSubstring) {
          expect(
            result.debugInfo ?? "",
            [
              `perl package/module driver contract vma mismatch: ${contractCase.id}`,
              `output=${JSON.stringify(result.output)}`,
              `debug=${result.debugInfo ?? ""}`,
            ].join("\n"),
          ).toContain(`pc_vma=${contractCase.expectedFaultVmaSubstring}`);
          expect(
            result.debugInfo ?? "",
            [
              `perl package/module driver contract fault vma mismatch: ${contractCase.id}`,
              `output=${JSON.stringify(result.output)}`,
              `debug=${result.debugInfo ?? ""}`,
            ].join("\n"),
          ).toContain(`fault_vma=${contractCase.expectedFaultVmaSubstring}`);
        }

        if (contractCase.expectHeapJumpAfterBrk) {
          const debugInfo = result.debugInfo ?? "";
          const faultPc = parseDebugHexField(debugInfo, "fault_pc");
          const memBrkBase = parseDebugHexField(debugInfo, "mem_brk_base");
          const memBrkCurrent = parseDebugHexField(debugInfo, "mem_brk_current");
          const syscall = parseDebugIntField(debugInfo, "syscall");
          const memNr = parseDebugIntField(debugInfo, "mem_nr");

          expect(
            faultPc,
            [
              `perl package/module driver contract fault_pc missing: ${contractCase.id}`,
              `debug=${debugInfo}`,
            ].join("\n"),
          ).not.toBeNull();
          expect(
            memBrkBase,
            [
              `perl package/module driver contract mem_brk_base missing: ${contractCase.id}`,
              `debug=${debugInfo}`,
            ].join("\n"),
          ).not.toBeNull();
          expect(
            memBrkCurrent,
            [
              `perl package/module driver contract mem_brk_current missing: ${contractCase.id}`,
              `debug=${debugInfo}`,
            ].join("\n"),
          ).not.toBeNull();
          expect(
            syscall,
            [
              `perl package/module driver contract syscall missing: ${contractCase.id}`,
              `debug=${debugInfo}`,
            ].join("\n"),
          ).toBe(214);
          expect(
            memNr,
            [
              `perl package/module driver contract mem_nr missing: ${contractCase.id}`,
              `debug=${debugInfo}`,
            ].join("\n"),
          ).toBe(214);
          expect(
            faultPc! >= memBrkBase! && faultPc! < memBrkCurrent!,
            [
              `perl package/module driver contract expected heap jump after brk: ${contractCase.id}`,
              `fault_pc=0x${faultPc?.toString(16) ?? "null"}`,
              `mem_brk_base=0x${memBrkBase?.toString(16) ?? "null"}`,
              `mem_brk_current=0x${memBrkCurrent?.toString(16) ?? "null"}`,
              `debug=${debugInfo}`,
            ].join("\n"),
          ).toBe(true);
        }
      }
    } finally {
      runtime.destroy();
    }
  }, 20_000);
}
