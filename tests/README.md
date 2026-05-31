# runtime/tests

top-level は次の 3 つだけに固定する。

- `runtime/`
  - runtime invariant
  - ownership boundary
  - parent/child lifecycle
  - fd/pipe/epoll/socket/stdio/fs の一般契約
- `workloads/`
  - language / toolchain 固有 smoke
  - 例: `go build`, `go env`, `go tool compile`
- `support/`
  - shared harness
  - worker URL rewrite, common spawn helper, snapshot helper など

ルール:

- test の所属は `何を検証しているか` で決める
- helper / fixture の実装言語では決めない
- top-level `tests/fixtures/...` は作らない
- generic runtime invariant を `workloads/<language>/...` に置かない
- language 固有 fixture は `workloads/<language>/fixtures/...` に閉じる

代表:

- `runtime/api/`
  - public Runtime / Process contract
- `runtime/worker/`
  - lifecycle, execve handoff, vfork/execve orchestration
- `runtime/kernel-worker/`
  - shared/private state sync
- `runtime/fs/`
  - MemFS / runtime-owned snapshot sync

## workload artifacts

workload tests は既存の `TIDEMARK_TEST_*` environment contract を維持する。
artifact 準備の共通処理は `scripts/prepare-workload-artifacts.mjs` に置き、
個別の workload test には置かない。

```bash
pnpm run prepare:workloads -- \
  --manifest ../artifacts/manifests/runtime-workloads.json \
  --profile core-smoke
```

script は `artifacts` repository の pin 済み GitHub release asset を
download し、SHA256 を検証し、payload を `.artifacts/cache` に展開して
`.artifacts/workload-env.json` を書く。test setup file は workload module
の import 前にこの env file を読む。明示的に指定された environment variable
は生成値より優先される。

apt の `Packages.xz` response body のように `payload.tar.zst` 外の入力が
必要な workload では、manifest entry が `packageIndex.sha256` でその入力を
明示 pin するか、`sourceManifestSha256` を pin している場合だけ release
source metadata を使う。
