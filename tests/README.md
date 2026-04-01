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
