import { expect, test } from "vite-plus/test";
import {
  bucketPackageModuleDriverFailure,
  preparePackageModuleDriverRuntime,
  runPackageModuleDriverEntry,
  validatePackageModuleDriverOutput,
} from "../../../support/package-module-driver-harness.js";
import { perlProvePackageModuleEntry } from "./package-module-driver.js";

const perlIsolationEntries = [
  {
    ...perlProvePackageModuleEntry,
    id: "perl-package-driver-lower-bound",
    argv: ["/usr/bin/perl", "-e", 'print "perl-ok\\n";'],
    expectedDriverSubstrings: ["perl-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-app-prove-module-load",
    argv: ["/usr/bin/perl", "-MApp::Prove", "-e", 'print "app-prove-ok\\n";'],
    expectedDriverSubstrings: ["app-prove-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test-more-module-load",
    argv: ["/usr/bin/perl", "-MTest::More", "-e", 'print "test-more-ok\\n";'],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-tap-harness-module-load",
    argv: ["/usr/bin/perl", "-MTAP::Harness", "-e", 'print "tap-harness-ok\\n";'],
    expectedDriverSubstrings: ["tap-harness-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-module-load",
    argv: ["/usr/bin/perl", "-MTest2::API", "-e", 'print "test2-api-ok\\n";'],
    expectedDriverSubstrings: ["test2-api-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-stack-object",
    argv: [
      "/usr/bin/perl",
      "-MTest2::API",
      "-e",
      'my $stack = Test2::API::test2_stack(); print ref($stack) . "\\n";',
    ],
    expectedDriverSubstrings: ["Test2::API::Stack"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-stack-peek",
    argv: [
      "/usr/bin/perl",
      "-MTest2::API",
      "-e",
      'my $top = Test2::API::test2_stack()->peek; print(defined($top) ? "test2-api-stack-peek-defined\\n" : "test2-api-stack-peek-undef\\n");',
    ],
    expectedDriverSubstrings: ["test2-api-stack-peek-undef"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-stdout-getter",
    argv: [
      "/usr/bin/perl",
      "-MTest2::API",
      "-e",
      'my $fh = Test2::API::test2_stdout(); print((defined(fileno($fh)) ? "test2-api-stdout-getter-ok\\n" : "test2-api-stdout-getter-missing\\n"));',
    ],
    expectedDriverSubstrings: ["test2-api-stdout-getter-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-stderr-getter",
    argv: [
      "/usr/bin/perl",
      "-MTest2::API",
      "-e",
      'my $fh = Test2::API::test2_stderr(); print((defined(fileno($fh)) ? "test2-api-stderr-getter-ok\\n" : "test2-api-stderr-getter-missing\\n"));',
    ],
    expectedDriverSubstrings: ["test2-api-stderr-getter-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-clone-stdout",
    argv: [
      "/usr/bin/perl",
      "-MTest2::API",
      "-MTest2::Util=clone_io",
      "-e",
      'my $fh = clone_io(Test2::API::test2_stdout()); print((defined(fileno($fh)) ? "test2-api-clone-stdout-ok\\n" : "test2-api-clone-stdout-missing\\n"));',
    ],
    expectedDriverSubstrings: ["test2-api-clone-stdout-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-clone-stderr",
    argv: [
      "/usr/bin/perl",
      "-MTest2::API",
      "-MTest2::Util=clone_io",
      "-e",
      'my $fh = clone_io(Test2::API::test2_stderr()); print((defined(fileno($fh)) ? "test2-api-clone-stderr-ok\\n" : "test2-api-clone-stderr-missing\\n"));',
    ],
    expectedDriverSubstrings: ["test2-api-clone-stderr-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-clone-stdout-autoflush-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::API",
      "-MTest2::Util=clone_io",
      "-e",
      'my $fh = clone_io(Test2::API::test2_stdout()); my $old = select $fh; $| = 1; select $old; print "test2-api-clone-stdout-autoflush-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-clone-stdout-select-only-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::API",
      "-MTest2::Util=clone_io",
      "-e",
      'my $fh = clone_io(Test2::API::test2_stdout()); my $old = select $fh; select $old; print "test2-api-clone-stdout-select-only-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-clone-stdout-read-autoflush-state-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::API",
      "-MTest2::Util=clone_io",
      "-e",
      'my $fh = clone_io(Test2::API::test2_stdout()); my $old = select $fh; my $af = $|; select $old; print "test2-api-clone-stdout-read-autoflush-state-posix-exit-$af\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-clone-stderr-autoflush-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::API",
      "-MTest2::Util=clone_io",
      "-e",
      'my $fh = clone_io(Test2::API::test2_stderr()); my $old = select $fh; $| = 1; select $old; print "test2-api-clone-stderr-autoflush-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-clone-stderr-select-only-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::API",
      "-MTest2::Util=clone_io",
      "-e",
      'my $fh = clone_io(Test2::API::test2_stderr()); my $old = select $fh; select $old; print "test2-api-clone-stderr-select-only-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-clone-stderr-read-autoflush-state-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::API",
      "-MTest2::Util=clone_io",
      "-e",
      'my $fh = clone_io(Test2::API::test2_stderr()); my $old = select $fh; my $af = $|; select $old; print "test2-api-clone-stderr-read-autoflush-state-posix-exit-$af\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-clone-both-autoflush-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::API",
      "-MTest2::Util=clone_io",
      "-e",
      'my $out = clone_io(Test2::API::test2_stdout()); my $old = select $out; $| = 1; select $old; my $err = clone_io(Test2::API::test2_stderr()); $old = select $err; $| = 1; select $old; print "test2-api-clone-both-autoflush-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-stdio-autoflush",
    argv: [
      "/usr/bin/perl",
      "-e",
      'my $old = select STDOUT; $| = 1; select STDERR; $| = 1; select $old; print "perl-stdio-autoflush-ok\\n";',
    ],
    expectedDriverSubstrings: ["perl-stdio-autoflush-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-stdio-dup-stdout-autoflush-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-e",
      'open(my $fh, ">&", fileno(STDOUT)) or die "dup stdout failed: $!"; my $old = select $fh; $| = 1; select $old; print "perl-stdio-dup-stdout-autoflush-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-util-clone-stdout-autoflush-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::Util=clone_io",
      "-e",
      'my $fh = clone_io(\\*STDOUT); my $old = select $fh; $| = 1; select $old; print "perl-test2-util-clone-stdout-autoflush-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-stdout-autoflush-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::API",
      "-e",
      'my $fh = Test2::API::test2_stdout(); my $old = select $fh; $| = 1; select $old; print "perl-test2-api-stdout-autoflush-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-tap-formatter-base-namespace-load",
    argv: ["/usr/bin/perl", "-MTAP::Formatter::Base", "-e", 'print "tap-formatter-base-ok\\n";'],
    expectedDriverSubstrings: ["tap-formatter-base-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-tap-formatter-base-namespace-load-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTAP::Formatter::Base",
      "-e",
      'print "tap-formatter-base-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-tap-formatter-console-namespace-load",
    argv: ["/usr/bin/perl", "-MTAP::Formatter::Console", "-e", 'print "tap-formatter-console-ok\\n";'],
    expectedDriverSubstrings: ["tap-formatter-console-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-tap-formatter-console-namespace-load-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTAP::Formatter::Console",
      "-e",
      'print "tap-formatter-console-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-synthetic-test2-formatter-tap-namespace-load",
    argv: [
      "/usr/bin/perl",
      "-e",
      'package Synthetic::Test2::Formatter::TAP; use strict; use warnings; use Test2::Util qw/clone_io/; use Test2::Util::HashBase qw{ no_numbers handles _encoding _last_fh -made_assertion }; BEGIN { require Test2::Formatter; our @ISA = qw(Test2::Formatter) } sub _autoflush { my ($fh) = @_; my $old_fh = select $fh; $| = 1; select $old_fh; } _autoflush(\\*STDOUT); _autoflush(\\*STDERR); package main; print "synthetic-test2-formatter-tap-ok\\n";',
    ],
    expectedDriverSubstrings: ["synthetic-test2-formatter-tap-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-synthetic-test2-formatter-tap-open-handles-preloaded-test2-api-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::API",
      "-e",
      'package Synthetic::Test2::Formatter::TAP; use strict; use warnings; use Test2::Util qw/clone_io/; use Test2::Util::HashBase qw{ no_numbers handles _encoding _last_fh -made_assertion }; BEGIN { require Test2::Formatter; our @ISA = qw(Test2::Formatter) } sub _autoflush { my ($fh) = @_; my $old_fh = select $fh; $| = 1; select $old_fh; } _autoflush(\\*STDOUT); _autoflush(\\*STDERR); sub _open_handles { my $self = shift; my $out = clone_io(Test2::API::test2_stdout()); my $err = clone_io(Test2::API::test2_stderr()); _autoflush($out); _autoflush($err); return [$out, $err]; } package main; my $formatter = bless({}, "Synthetic::Test2::Formatter::TAP"); $formatter->_open_handles(); print "synthetic-test2-formatter-tap-open-handles-preloaded-test2-api-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-formatter-tap-require-only",
    argv: [
      "/usr/bin/perl",
      "-e",
      'require Test2::Formatter::TAP; print "test2-formatter-tap-require-only-ok\\n";',
    ],
    expectedDriverSubstrings: ["test2-formatter-tap-require-only-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-formatter-tap-namespace-load-noimport",
    argv: [
      "/usr/bin/perl",
      "-e",
      'use Test2::Formatter::TAP (); print "test2-formatter-tap-namespace-load-noimport-ok\\n";',
    ],
    expectedDriverSubstrings: ["test2-formatter-tap-namespace-load-noimport-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-formatter-import-add-tap-class",
    argv: [
      "/usr/bin/perl",
      "-MTest2::Formatter",
      "-e",
      'Test2::Formatter::import("Test2::Formatter::TAP"); print "test2-formatter-import-add-tap-class-ok\\n";',
    ],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-formatter-tap-namespace-load",
    argv: ["/usr/bin/perl", "-MTest2::Formatter::TAP", "-e", 'print "test2-formatter-tap-ok\\n";'],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-formatter-tap-namespace-load-posix-exit",
    argv: [
      "/usr/bin/perl",
      "-MPOSIX",
      "-MTest2::Formatter::TAP",
      "-e",
      'print "test2-formatter-tap-posix-exit-ok\\n"; POSIX::_exit(0);',
    ],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-formatter-tap-new-root",
    argv: [
      "/usr/bin/perl",
      "-MTest2::Formatter::TAP",
      "-e",
      'Test2::Formatter::TAP->new_root(); print "test2-formatter-tap-new-root-ok\\n";',
    ],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-new-hub-no-formatter-ipc",
    argv: [
      "/usr/bin/perl",
      "-MTest2::API",
      "-e",
      'my $hub = Test2::API::test2_stack()->new_hub(formatter => undef, ipc => undef); print ref($hub) . "\\n";',
    ],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-stack-top",
    argv: ["/usr/bin/perl", "-MTest2::API", "-e", 'Test2::API::test2_stack()->top; print "test2-api-stack-top-ok\\n";'],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-api-stack-top-posix-exit",
    argv: ["/usr/bin/perl", "-MPOSIX", "-MTest2::API", "-e", 'Test2::API::test2_stack()->top; print "test2-api-stack-top-posix-exit-ok\\n"; POSIX::_exit(0);'],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-formatter-namespace-load",
    argv: ["/usr/bin/perl", "-MTest2::Formatter", "-e", 'print "test2-formatter-ok\\n";'],
    expectedDriverSubstrings: ["test2-formatter-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-event-subtest-module-load",
    argv: ["/usr/bin/perl", "-MTest2::Event::Subtest", "-e", 'print "test2-event-subtest-ok\\n";'],
    expectedDriverSubstrings: ["test2-event-subtest-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test2-hub-subtest-module-load",
    argv: ["/usr/bin/perl", "-MTest2::Hub::Subtest", "-e", 'print "test2-hub-subtest-ok\\n";'],
    expectedDriverSubstrings: ["test2-hub-subtest-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test-builder-tododiag-module-load",
    argv: ["/usr/bin/perl", "-MTest::Builder::TodoDiag", "-e", 'print "test-builder-tododiag-ok\\n";'],
    expectedDriverSubstrings: ["test-builder-tododiag-ok"],
    expectTimeout: false,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test-builder-module-load",
    argv: ["/usr/bin/perl", "-MTest::Builder", "-e", 'print "test-builder-ok\\n";'],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test-builder-no-early-init-module-load",
    argv: ["/usr/bin/perl", "-MTest::Builder", "-e", 'print "test-builder-no-early-init-ok\\n";'],
    env: ["TB_NO_EARLY_INIT=1"],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test-builder-no-early-init-new",
    argv: ["/usr/bin/perl", "-MTest::Builder", "-e", 'Test::Builder->new; print "test-builder-no-early-init-new-ok\\n";'],
    env: ["TB_NO_EARLY_INIT=1"],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test-builder-posix-exit",
    argv: ["/usr/bin/perl", "-MPOSIX", "-MTest::Builder", "-e", 'print "test-builder-posix-exit-ok\\n"; POSIX::_exit(0);'],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-basic-test-direct",
    argv: ["/usr/bin/perl", "/work/basic.t"],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test-more-plan-run",
    argv: ["/usr/bin/perl", "-MTest::More", "-e", 'plan tests => 1; ok(1, "test-more-evidence");'],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-test-more-posix-exit",
    argv: ["/usr/bin/perl", "-MPOSIX", "-MTest::More", "-e", 'print "test-more-posix-exit-ok\\n"; POSIX::_exit(0);'],
    expectedDriverSubstrings: [],
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
  {
    ...perlProvePackageModuleEntry,
    id: "perl-prove-basic-runner",
    expectedTimeoutSubstrings: [],
    expectTimeout: true,
  },
] as const;

test("perl package/module driver isolation matrix", async () => {
  const prepared = await preparePackageModuleDriverRuntime(perlProvePackageModuleEntry);
  if (!prepared) {
    return;
  }

  const { runtime, executableBytes } = prepared;
  try {
    for (const entry of perlIsolationEntries) {
      const executionEntry = {
        ...entry,
        timeoutMs: entry.expectTimeout ? 10_000 : entry.timeoutMs,
      };
      if (entry.expectTimeout) {
        await runPackageModuleDriverEntry(runtime, executionEntry, executableBytes).then(
          (result) => {
            expect(
              result.exitCode,
              [
                `perl package/module driver isolation recovered exit mismatch: ${entry.id}`,
                `bucket=${bucketPackageModuleDriverFailure({ result })}`,
                `output=${JSON.stringify(result.output)}`,
                `debug=${result.debugInfo ?? ""}`,
              ].join("\n"),
            ).toBe(0);
          },
          (error) => {
            const bucket = bucketPackageModuleDriverFailure({ error });
            const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
            expect(
              detail,
              `perl package/module driver isolation timeout mismatch: ${entry.id}\nbucket=${bucket}`,
            ).toMatch(/timeout after \d+ms/i);
            for (const expected of entry.expectedTimeoutSubstrings ?? []) {
              expect(
                detail,
                [
                  `perl package/module driver isolation timeout output mismatch: ${entry.id}`,
                  `bucket=${bucket}`,
                  detail,
                ].join("\n"),
              ).toContain(expected);
            }
            expect(["unknown", "runtime-ownership"]).toContain(bucket);
          },
        );
        continue;
      }

      const result = await runPackageModuleDriverEntry(runtime, executionEntry, executableBytes).catch((error) => {
        const bucket = bucketPackageModuleDriverFailure({ error });
        throw new Error(
          [
            `perl package/module driver isolation failed: ${entry.id}`,
            `bucket=${bucket}`,
            error instanceof Error ? (error.stack ?? error.message) : String(error),
          ].join("\n"),
        );
      });

      const mismatches = validatePackageModuleDriverOutput(executionEntry, result.output);
      expect(
        result.exitCode,
        [
          `perl package/module driver isolation exit mismatch: ${entry.id}`,
          `bucket=${bucketPackageModuleDriverFailure({ result })}`,
          `exit=${result.exitCode}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toBe(0);
      expect(
        mismatches,
        [
          `perl package/module driver isolation output mismatch: ${entry.id}`,
          `bucket=${bucketPackageModuleDriverFailure({ result })}`,
          `output=${JSON.stringify(result.output)}`,
          `debug=${result.debugInfo ?? ""}`,
        ].join("\n"),
      ).toEqual([]);
    }
  } finally {
    runtime.destroy();
  }
}, 410_000);
