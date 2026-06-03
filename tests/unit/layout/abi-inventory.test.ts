import { describe, expect, test } from "vite-plus/test";
import * as fdLayout from "../../../src/layout/fd.js";
import * as fsEffectsLayout from "../../../src/layout/fs-effects.js";
import * as fsRingBuffer from "../../../src/fs/ring-buffer.js";
import * as fsRingLayout from "../../../src/layout/fs-ring.js";
import * as memfsLayout from "../../../src/layout/memfs.js";
import * as netBufferLayout from "../../../src/layout/net-buffer.js";
import * as ofdLayout from "../../../src/layout/ofd.js";
import * as pipeLayout from "../../../src/layout/pipe.js";
import * as pollLayout from "../../../src/layout/poll.js";
import * as runtimeStateLayout from "../../../src/layout/runtime-state.js";
import * as sabPageCache from "../../../src/fs/sab-page-cache.js";
import * as socketLayout from "../../../src/layout/socket.js";

type Classification = "abi-test-covered" | "explicit-gap";

type InventoryEntry = {
  readonly module: keyof typeof constantModules;
  readonly name: string;
  readonly classification: Classification;
  readonly protectedBy?: readonly string[];
  readonly reason?: string;
};

const constantModules = {
  fd: fdLayout,
  "fs-effects": fsEffectsLayout,
  "fs-ring-buffer": fsRingBuffer,
  "fs-ring": fsRingLayout,
  memfs: memfsLayout,
  "net-buffer": netBufferLayout,
  ofd: ofdLayout,
  pipe: pipeLayout,
  poll: pollLayout,
  "runtime-state": runtimeStateLayout,
  "sab-page-cache": sabPageCache,
  socket: socketLayout,
} as const;

const bridgeRuntimeStateTest = "tests/unit/bridge/runtime-state.test.ts";
const kernelObjectsTest = "tests/unit/layout/kernel-objects.test.ts";
const fsRingTest = "tests/unit/layout/fs-ring.test.ts";
const sabPageCacheTest = "tests/unit/fs/sab-page-cache.test.ts";

const inventory: readonly InventoryEntry[] = [
  { module: "fd", name: "FD_ENTRY_BYTES", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "fd", name: "FD_ENTRY_OFD_ID_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },

  { module: "fs-effects", name: "FS_EFFECT_MEMFS_MUTATION", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-effects", name: "FS_EFFECT_SHARED_OFD_STATE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-effects", name: "FS_EFFECT_MEMFS_FILE_DATA", classification: "abi-test-covered", protectedBy: [fsRingTest] },

  { module: "fs-ring", name: "FS_RING_SUBMIT_HEAD_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_SUBMIT_TAIL_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_COMPLETE_HEAD_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_COMPLETE_TAIL_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_NOTIFY_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_HEADER_BYTES", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_SUBMIT_QUEUE_SIZE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_COMPLETE_QUEUE_SIZE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_BYTES", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_RESPONSE_BYTES", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_OPCODE_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_FD_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_LEN_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_BUF_PTR_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_PATH_PTR_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_VM_PTR_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_OFFSET_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_FLAGS_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_REQUEST_MODE_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_RESPONSE_RESULT_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_RESPONSE_VM_PTR_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_COMPLETE_QUEUE_OFFSET", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_RING_TOTAL_BYTES", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_READ", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_WRITE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_OPENAT", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_MKDIR", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_UNLINK", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_CLOSE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_FSTAT", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_GETDENTS", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_LSEEK", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_MMAP", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_EXECVE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_RENAME", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_OP_TRUNCATE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_REQ_FLAG_PWRITE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring", name: "FS_REQ_FLAG_WRITEV", classification: "abi-test-covered", protectedBy: [fsRingTest] },

  { module: "fs-ring-buffer", name: "RING_BUFFER_SIZE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring-buffer", name: "OFF_SUBMIT_HEAD", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring-buffer", name: "OFF_SUBMIT_TAIL", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring-buffer", name: "OFF_COMPLETE_HEAD", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring-buffer", name: "OFF_COMPLETE_TAIL", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring-buffer", name: "OFF_NOTIFY", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring-buffer", name: "OFF_SQ_BASE", classification: "abi-test-covered", protectedBy: [fsRingTest] },
  { module: "fs-ring-buffer", name: "OFF_CQ_BASE", classification: "abi-test-covered", protectedBy: [fsRingTest] },

  { module: "memfs", name: "MEMFS_HEADER_BYTES_FALLBACK", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_HEADER_OFF_INODE_COUNT", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_HEADER_OFF_DIR_ENTRY_COUNT", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_HEADER_OFF_DATA_USED", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_HEADER_OFF_DATA_CAP", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_HEADER_OFF_INODES_PTR", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_HEADER_OFF_FIRST_CHILD_PTR", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_HEADER_OFF_DIR_ENTRIES_PTR", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_HEADER_OFF_DATA_PTR", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_INODE_OFF_MODE", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_INODE_OFF_SIZE", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_INODE_OFF_DATA_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "memfs", name: "MEMFS_INODE_OFF_DATA_CAP", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },

  { module: "net-buffer", name: "NET_BUFFER_HEADER_BYTES", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "net-buffer", name: "NET_BUFFER_HEAD_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "net-buffer", name: "NET_BUFFER_TAIL_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "net-buffer", name: "NET_BUFFER_CAPACITY_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "net-buffer", name: "NET_BUFFER_DATA_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },

  { module: "ofd", name: "OFD_TYPE_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "ofd", name: "OFD_HOST_FD_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "ofd", name: "OFD_REFCOUNT_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "ofd", name: "OFD_OFFSET_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "ofd", name: "OFD_INO_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },

  { module: "pipe", name: "PIPE_HEADER_BYTES", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "pipe", name: "PIPE_HEAD_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "pipe", name: "PIPE_TAIL_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "pipe", name: "PIPE_READ_OPEN_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "pipe", name: "PIPE_WRITE_OPEN_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "pipe", name: "PIPE_GENERATION_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "pipe", name: "PIPE_DATA_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "pipe", name: "PIPE_BUFFER_BYTES", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "pipe", name: "PIPE_SLOT_BYTES", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  {
    module: "pipe",
    name: "PIPE_SLOT_MODE_READ",
    classification: "explicit-gap",
    reason: "Runtime-local pipe snapshot/sync mode bit. The kernel owns pipe slot memory layout but does not expose or consume these runtime sync-mode flags.",
  },
  {
    module: "pipe",
    name: "PIPE_SLOT_MODE_WRITE",
    classification: "explicit-gap",
    reason: "Runtime-local pipe snapshot/sync mode bit. The kernel owns pipe slot memory layout but does not expose or consume these runtime sync-mode flags.",
  },
  {
    module: "pipe",
    name: "PIPE_SLOT_MODE_CONTROL",
    classification: "explicit-gap",
    reason: "Runtime-local pipe snapshot/sync mode bit. The kernel owns pipe slot memory layout but does not expose or consume these runtime sync-mode flags.",
  },

  { module: "poll", name: "POLLFD_BYTES", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },

  {
    module: "runtime-state",
    name: "RUNTIME_BRIDGE_NOTIFY_INDEX",
    classification: "explicit-gap",
    protectedBy: [bridgeRuntimeStateTest],
    reason: "Runtime-owned bridge notification slot. The kernel does not consume this SharedArrayBuffer protocol; bridge round-trip tests protect the runtime ABI.",
  },
  {
    module: "runtime-state",
    name: "RUNTIME_BRIDGE_EPOCH_INDEX",
    classification: "explicit-gap",
    protectedBy: [bridgeRuntimeStateTest],
    reason: "Runtime-owned bridge epoch slot. The kernel does not consume this SharedArrayBuffer protocol; bridge epoch tests protect the runtime ABI.",
  },
  {
    module: "runtime-state",
    name: "RUNTIME_BRIDGE_DEBUG_STAGE_INDEX",
    classification: "explicit-gap",
    protectedBy: [bridgeRuntimeStateTest],
    reason: "Runtime-owned bridge debug slot. The kernel does not consume this SharedArrayBuffer protocol; bridge debug state is a runtime-local ABI.",
  },
  {
    module: "runtime-state",
    name: "RUNTIME_BRIDGE_DEBUG_VALUE_INDEX",
    classification: "explicit-gap",
    protectedBy: [bridgeRuntimeStateTest],
    reason: "Runtime-owned bridge debug value slot. The kernel does not consume this SharedArrayBuffer protocol; bridge debug state is a runtime-local ABI.",
  },
  {
    module: "runtime-state",
    name: "RUNTIME_STATE_VERSION_INDEX",
    classification: "explicit-gap",
    protectedBy: [bridgeRuntimeStateTest],
    reason: "Runtime-owned seqlock slot for publishing KernelRuntimeState between runtime workers; kernel exports the state values, not this bridge layout.",
  },
  {
    module: "runtime-state",
    name: "RUNTIME_STATE_HEADER_BYTES",
    classification: "explicit-gap",
    protectedBy: [bridgeRuntimeStateTest],
    reason: "Runtime-owned KernelRuntimeState bridge header size. The kernel exports state fields, while runtime bridge tests protect the publication layout.",
  },
  {
    module: "runtime-state",
    name: "RUNTIME_STATE_FIELD_COUNT",
    classification: "explicit-gap",
    protectedBy: [bridgeRuntimeStateTest],
    reason: "Runtime-owned KernelRuntimeState bridge field count. The kernel exports state fields, while runtime bridge tests protect the publication layout.",
  },
  {
    module: "runtime-state",
    name: "RUNTIME_STATE_FIELD_OFFSETS",
    classification: "explicit-gap",
    protectedBy: [bridgeRuntimeStateTest],
    reason: "Runtime-owned KernelRuntimeState bridge field offsets. The kernel exports state fields, while runtime bridge tests protect the publication layout.",
  },

  {
    module: "sab-page-cache",
    name: "MAX_INODES",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache capacity for kernel-worker published file data; no kernel ABI export exists for this cache format.",
  },
  {
    module: "sab-page-cache",
    name: "HEADER_SIZE",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache header size for kernel-worker published file data; page-cache tests protect the runtime cache format.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_ENTRY_SIZE",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode entry size; no kernel ABI export exists because the cache is runtime-owned.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_TABLE_SIZE",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode table size; page-cache tests protect high inode and bounds behavior.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_TABLE_OFFSET",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode table offset; page-cache tests protect metadata publication and reads.",
  },
  {
    module: "sab-page-cache",
    name: "DATA_REGION_OFFSET",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache data-region offset; page-cache tests protect data allocation, reads, and writes.",
  },
  {
    module: "sab-page-cache",
    name: "OFF_DATA_USED",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache header field offset; no kernel ABI export exists for this cache protocol.",
  },
  {
    module: "sab-page-cache",
    name: "OFF_INODE_COUNT",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache header field offset; page-cache tests protect inode count publication.",
  },
  {
    module: "sab-page-cache",
    name: "OFF_DIR_EPOCH",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache directory epoch field offset; no kernel ABI export exists for this cache protocol.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_OFF_VERSION",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode seqlock offset; page-cache tests protect stable metadata reads.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_OFF_DATA_OFF",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode data offset field; page-cache tests protect file data publication.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_OFF_DATA_SIZE",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode size field; page-cache tests protect metadata and read bounds.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_OFF_MODE",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode mode field; page-cache tests protect metadata publication.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_OFF_PARENT",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode parent field; page-cache tests protect inode record publication.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_OFF_NAME_OFF",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode name offset field; page-cache tests protect inode record names.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_OFF_NAME_LEN",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode name length field; page-cache tests protect inode record names.",
  },
  {
    module: "sab-page-cache",
    name: "INODE_OFF_NEXT_SIBLING",
    classification: "explicit-gap",
    protectedBy: [sabPageCacheTest],
    reason: "Runtime-owned SAB page-cache inode sibling field; page-cache tests protect inode record publication.",
  },

  { module: "socket", name: "SOCKET_STATE_CONNECTED", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "socket", name: "SOCKET_STATE_SHUTDOWN", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "socket", name: "SOCKET_SHUT_RD_WR_MASK", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "socket", name: "SOCKET_SLOT_STATE_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "socket", name: "SOCKET_SLOT_EXTERNAL_PEER_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "socket", name: "SOCKET_SLOT_RECV_BUF_IDX_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
  { module: "socket", name: "SOCKET_SLOT_SHUTDOWN_MASK_OFFSET", classification: "abi-test-covered", protectedBy: [kernelObjectsTest] },
];

function constantExports(module: Record<string, unknown>): string[] {
  return Object.entries(module)
    .filter(([, value]) => typeof value !== "function")
    .map(([name]) => name)
    .sort();
}

describe("layout ABI inventory", () => {
  test("classifies every runtime-side layout constant", () => {
    const exportedConstants = Object.entries(constantModules).flatMap(([module, exports]) =>
      constantExports(exports).map((name) => `${module}:${name}`)
    ).sort();
    const inventoryKeys = inventory.map(({ module, name }) => `${module}:${name}`).sort();
    const duplicateKeys = inventoryKeys.filter((key, index) => inventoryKeys.indexOf(key) !== index);

    expect(duplicateKeys).toEqual([]);
    expect(inventoryKeys).toEqual(exportedConstants);
  });

  test("records validation source or explicit gap for every layout constant", () => {
    const invalidEntries = inventory.flatMap((entry) => {
      if (entry.classification === "abi-test-covered") {
        return entry.protectedBy?.length ? [] : [`${entry.module}:${entry.name} missing ABI validation test`];
      }
      if (entry.classification === "explicit-gap") {
        return entry.reason && entry.reason.length >= 80 ? [] : [`${entry.module}:${entry.name} missing explicit gap reason`];
      }
      return [`${entry.module}:${entry.name} has unsupported classification`];
    });

    expect(invalidEntries).toEqual([]);
  });

  test("keeps runtime-local pipe sync mode gaps non-overlapping", () => {
    expect(pipeLayout.PIPE_SLOT_MODE_READ).toBe(1);
    expect(pipeLayout.PIPE_SLOT_MODE_WRITE).toBe(2);
    expect(pipeLayout.PIPE_SLOT_MODE_CONTROL).toBe(4);
    expect(pipeLayout.PIPE_SLOT_MODE_READ & pipeLayout.PIPE_SLOT_MODE_WRITE).toBe(0);
    expect(pipeLayout.PIPE_SLOT_MODE_READ & pipeLayout.PIPE_SLOT_MODE_CONTROL).toBe(0);
    expect(pipeLayout.PIPE_SLOT_MODE_WRITE & pipeLayout.PIPE_SLOT_MODE_CONTROL).toBe(0);
  });
});
