# runtime/tests

The test tree is split by suite responsibility:

- `unit/`
  - artifact-free runtime unit and contract tests
  - public Runtime / Process API contracts
  - worker lifecycle, fd/ofd/pipe/epoll/socket/fs ownership, ABI inventory
- `integration/runtime/`
  - language-neutral runtime integration probes
  - runtime behavior that is not owned by one support target
- `integration/targets/`
  - concrete support-target artifact tests
  - language runtimes, toolchains, package substrates, upstream suites, and
    binary targets
- `support/`
  - shared test harnesses and integration env loading
  - generic spawn, snapshot, stdout, timeout, and result helpers

Rules:

- Place tests by what they validate, not by the helper implementation language.
- Keep target-specific artifact paths, argv, version strings, sysroot layout,
  package-manager behavior, and expected output under
  `integration/targets/<target>/`.
- Keep language-neutral runtime probes under `integration/runtime/<area>/`.
- Keep shared mechanics in `support/`; do not create a language registry there.
- Do not add top-level `tests/fixtures/...`.
- Missing required artifacts fail in integration tests. They are not successful
  skips.
- Intentionally disabled observation gates must be registered as explicit
  skipped tests, usually with `test.skip` selected from the gate env variable.

## Commands

```bash
pnpm test
pnpm run test:unit
pnpm run test:integration
pnpm run test:integration:runtime
pnpm run test:integration:targets
pnpm run test:integration:focus -- <test-file>
pnpm run prepare:integration -- \
  --manifest ../artifacts/manifests/runtime-workloads.json \
  --profile <profile>
```

`pnpm test` and `pnpm run test:unit` run only `tests/unit`.

`pnpm run test:integration:focus -- <test-file>` runs a selected integration
test through the same integration environment setup as the broader integration
package scripts.

`prepare:integration` downloads pinned GitHub Release assets from the
`artifacts` repository manifest, verifies SHA256, extracts payloads into
`.artifacts/cache`, and writes `.artifacts/workload-env.json`.

Integration tests consume the generated `TIDEMARK_TEST_*` input variables. Test
files do not read the artifact repository, GitHub Releases, or manifest files
directly. Explicitly provided environment variables take precedence over values
from `.artifacts/workload-env.json`.

Artifact preparation proves input identity. It is not behavior coverage by
itself; behavior evidence comes from the relevant integration test run.
