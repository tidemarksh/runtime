import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vite-plus/test";
import {
  Runtime,
  logSkip,
  readKernelWasm,
  runtimeDebugSummary,
  spawnWithOutput,
} from "../support/runtime-workload.js";

const hostRiscvGcc = process.env.TIDEMARK_TEST_HOST_RISCV_GCC ?? "riscv64-linux-gnu-gcc";

function buildForkPipeDup2Probe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fork-pipe-dup2-"));
  const sourcePath = join(tmp, "fork_pipe_dup2.c");
  const outputPath = join(tmp, "fork-pipe-dup2");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int wait_for_text(int fd, const char *needle, char *buf, size_t cap, ssize_t *out_n) {",
      "  size_t off = 0;",
      "  if (cap == 0) return -2;",
      "  buf[0] = '\\0';",
      "  while (off + 1 < cap) {",
      "    ssize_t n = read(fd, buf + off, cap - 1 - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    if (n == 0) break;",
      "    off += (size_t)n;",
      "    buf[off] = '\\0';",
      "    if (strstr(buf, needle) != NULL) {",
      "      if (out_n) *out_n = (ssize_t)off;",
      "      return 0;",
      "    }",
      "  }",
      "  if (out_n) *out_n = (ssize_t)off;",
      "  return strstr(buf, needle) != NULL ? 0 : -2;",
      "}",
      "",
      "int main(void) {",
      "  for (int iter = 0; iter < 16; iter++) {",
      "    int in_pipe[2];",
      "    int out_pipe[2];",
      "    if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "    pid_t child = fork();",
      "    if (child < 0) { perror(\"fork\"); return 13; }",
      "    if (child == 0) {",
      "      if ((iter & 1) == 0) {",
      "        if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "        if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "      } else {",
      "        if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "        if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "      }",
      "      close(in_pipe[0]);",
      "      close(in_pipe[1]);",
      "      close(out_pipe[0]);",
      "      close(out_pipe[1]);",
      "      (void)open(\"/dev/null\", O_WRONLY);",
      "      if (write_all(1, \"child-pipe-ok\\n\", 14) != 0) _exit(127);",
      "      _exit(0);",
      "    }",
      "    close(in_pipe[0]);",
      "    close(out_pipe[1]);",
      "    close(in_pipe[1]);",
      "    char buf[64];",
      "    ssize_t n = 0;",
      "    int got = wait_for_text(out_pipe[0], \"child-pipe-ok\", buf, sizeof(buf), &n);",
      "    close(out_pipe[0]);",
      "    int status = 0;",
      "    waitpid(child, &status, 0);",
      "    if (got == -1) { perror(\"read-child\"); return 14; }",
      "    if (got != 0) {",
      "      fprintf(stderr, \"missing-child-output: iter=%d status=%d n=%zd output=%s\\n\", iter, status, n, buf);",
      "      return 15;",
      "    }",
      "    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "      fprintf(stderr, \"child-status: iter=%d status=%d\\n\", iter, status);",
      "      return 16;",
      "    }",
      "  }",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build fork pipe dup2 probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildForkPipeDup2ExecProbe(): { parent: Uint8Array; child: Uint8Array } | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fork-pipe-dup2-exec-"));
  const parentSourcePath = join(tmp, "fork_pipe_dup2_exec.c");
  const childSourcePath = join(tmp, "child_writer.c");
  const parentOutputPath = join(tmp, "fork-pipe-dup2-exec");
  const childOutputPath = join(tmp, "child-writer");
  try {
    writeFileSync(childSourcePath, [
      "#include <errno.h>",
      "#include <stddef.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  return write_all(1, \"child-exec-pipe-ok\\n\", 19) == 0 ? 0 : 17;",
      "}",
      "",
    ].join("\n"));
    writeFileSync(parentSourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "extern char **environ;",
      "",
      "static int wait_for_text(int fd, const char *needle, char *buf, size_t cap, ssize_t *out_n) {",
      "  size_t off = 0;",
      "  if (cap == 0) return -2;",
      "  buf[0] = '\\0';",
      "  while (off + 1 < cap) {",
      "    ssize_t n = read(fd, buf + off, cap - 1 - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    if (n == 0) break;",
      "    off += (size_t)n;",
      "    buf[off] = '\\0';",
      "    if (strstr(buf, needle) != NULL) {",
      "      if (out_n) *out_n = (ssize_t)off;",
      "      return 0;",
      "    }",
      "  }",
      "  if (out_n) *out_n = (ssize_t)off;",
      "  return strstr(buf, needle) != NULL ? 0 : -2;",
      "}",
      "",
      "int main(void) {",
      "  for (int iter = 0; iter < 8; iter++) {",
      "    int in_pipe[2];",
      "    int out_pipe[2];",
      "    if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "    pid_t child = fork();",
      "    if (child < 0) { perror(\"fork\"); return 13; }",
      "    if (child == 0) {",
      "      if ((iter & 1) == 0) {",
      "        if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "        if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "      } else {",
      "        if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "        if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "      }",
      "      close(in_pipe[0]); close(in_pipe[1]);",
      "      close(out_pipe[0]); close(out_pipe[1]);",
      "      (void)open(\"/dev/null\", O_WRONLY);",
      "      char *argv[] = { (char *)\"/child-writer\", NULL };",
      "      execve(\"/child-writer\", argv, environ);",
      "      perror(\"execve-child\");",
      "      _exit(127);",
      "    }",
      "    close(in_pipe[0]);",
      "    close(out_pipe[1]);",
      "    close(in_pipe[1]);",
      "    char buf[64];",
      "    ssize_t n = 0;",
      "    int got = wait_for_text(out_pipe[0], \"child-exec-pipe-ok\", buf, sizeof(buf), &n);",
      "    close(out_pipe[0]);",
      "    int status = 0;",
      "    waitpid(child, &status, 0);",
      "    if (got == -1) { perror(\"read-child\"); return 14; }",
      "    if (got != 0) {",
      "      fprintf(stderr, \"missing-child-output: iter=%d status=%d n=%zd output=%s\\n\", iter, status, n, buf);",
      "      return 15;",
      "    }",
      "    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "      fprintf(stderr, \"child-status: iter=%d status=%d\\n\", iter, status);",
      "      return 16;",
      "    }",
      "  }",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", childOutputPath, childSourcePath], {
      stdio: "pipe",
    });
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", parentOutputPath, parentSourcePath], {
      stdio: "pipe",
    });
    return {
      parent: new Uint8Array(readFileSync(parentOutputPath)),
      child: new Uint8Array(readFileSync(childOutputPath)),
    };
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build fork pipe dup2 exec probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildForkPipeDup2ExecStdinEofProbe(): { parent: Uint8Array; child: Uint8Array } | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fork-pipe-dup2-eof-"));
  const parentSourcePath = join(tmp, "fork_pipe_dup2_exec_eof.c");
  const childSourcePath = join(tmp, "child_read_until_eof.c");
  const parentOutputPath = join(tmp, "fork-pipe-dup2-exec-eof");
  const childOutputPath = join(tmp, "child-read-until-eof");
  try {
    writeFileSync(childSourcePath, [
      "#include <errno.h>",
      "#include <stddef.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  char buf[32];",
      "  size_t total = 0;",
      "  for (;;) {",
      "    ssize_t n = read(0, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return 18;",
      "    }",
      "    if (n == 0) break;",
      "    total += (size_t)n;",
      "  }",
      "  if (total != 17) return 19;",
      "  return write_all(1, \"child-stdin-eof-ok\\n\", 19) == 0 ? 0 : 20;",
      "}",
      "",
    ].join("\n"));
    writeFileSync(parentSourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "extern char **environ;",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  for (int iter = 0; iter < 8; iter++) {",
      "    int in_pipe[2];",
      "    int out_pipe[2];",
      "    if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "    pid_t child = fork();",
      "    if (child < 0) { perror(\"fork\"); return 13; }",
      "    if (child == 0) {",
      "      if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "      if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "      close(in_pipe[0]); close(in_pipe[1]);",
      "      close(out_pipe[0]); close(out_pipe[1]);",
      "      char *argv[] = { (char *)\"/child-read-until-eof\", NULL };",
      "      execve(\"/child-read-until-eof\", argv, environ);",
      "      perror(\"execve-child\");",
      "      _exit(127);",
      "    }",
      "    close(in_pipe[0]);",
      "    close(out_pipe[1]);",
      "    if (write_all(in_pipe[1], \"stdin-pipe-eof-ok\", 17) != 0) { perror(\"write-parent\"); return 14; }",
      "    close(in_pipe[1]);",
      "    char buf[64];",
      "    ssize_t n = read(out_pipe[0], buf, sizeof(buf) - 1);",
      "    close(out_pipe[0]);",
      "    int status = 0;",
      "    waitpid(child, &status, 0);",
      "    if (n < 0) { perror(\"read-child\"); return 15; }",
      "    buf[n] = '\\0';",
      "    if (strstr(buf, \"child-stdin-eof-ok\") == NULL) {",
      "      fprintf(stderr, \"missing-child-output: iter=%d status=%d n=%zd output=%s\\n\", iter, status, n, buf);",
      "      return 16;",
      "    }",
      "    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "      fprintf(stderr, \"child-status: iter=%d status=%d\\n\", iter, status);",
      "      return 17;",
      "    }",
      "  }",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", childOutputPath, childSourcePath], {
      stdio: "pipe",
    });
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", parentOutputPath, parentSourcePath], {
      stdio: "pipe",
    });
    return {
      parent: new Uint8Array(readFileSync(parentOutputPath)),
      child: new Uint8Array(readFileSync(childOutputPath)),
    };
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build fork pipe dup2 EOF probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildSequentialForkExecProtocolProbe(): { parent: Uint8Array; child: Uint8Array } | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-sequential-fork-protocol-"));
  const parentSourcePath = join(tmp, "sequential_fork_protocol.c");
  const childSourcePath = join(tmp, "child_protocol_keepalive.c");
  const parentOutputPath = join(tmp, "sequential-fork-protocol");
  const childOutputPath = join(tmp, "child-protocol-keepalive");
  try {
    writeFileSync(childSourcePath, [
      "#include <errno.h>",
      "#include <stddef.h>",
      "#include <string.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  char buf[256];",
      "  size_t total = 0;",
      "  int saw_request_end = 0;",
      "  if (write_all(1, \"100 Capabilities\\nVersion: 1.0\\n\\n\", 31) != 0) return 21;",
      "  while (!saw_request_end) {",
      "    ssize_t n = read(0, buf, sizeof(buf) - 1);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return 22;",
      "    }",
      "    if (n == 0) return 23;",
      "    buf[n] = '\\0';",
      "    total += (size_t)n;",
      "    if (strstr(buf, \"\\n\\n\") != NULL) saw_request_end = 1;",
      "    if (total > 4096) return 24;",
      "  }",
      "  if (write_all(1, \"201 URI Done\\nFilename: /tmp/probe\\n\\n\", 36) != 0) return 25;",
      "  for (;;) {",
      "    ssize_t n = read(0, buf, sizeof(buf));",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return 26;",
      "    }",
      "    if (n == 0) return 0;",
      "  }",
      "}",
      "",
    ].join("\n"));
    writeFileSync(parentSourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <poll.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "extern char **environ;",
      "",
      "struct child_handle {",
      "  pid_t pid;",
      "  int stdin_fd;",
      "  int stdout_fd;",
      "};",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int wait_for_text(int fd, const char *needle, char *out, size_t out_len) {",
      "  size_t used = 0;",
      "  if (out_len == 0) return -7;",
      "  out[0] = '\\0';",
      "  for (int iter = 0; iter < 400; iter++) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 25);",
      "    if (pr < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    if (pr == 0) continue;",
      "    if (pfd.revents & POLLIN) {",
      "      ssize_t n = read(fd, out + used, out_len - used - 1);",
      "      if (n < 0) {",
      "        if (errno == EINTR) continue;",
      "        return -2;",
      "      }",
      "      if (n == 0) return -3;",
      "      used += (size_t)n;",
      "      out[used] = '\\0';",
      "      if (strstr(out, needle) != NULL) return 0;",
      "      if (strstr(out, \"worker-before-close\") != NULL) return -9;",
      "      if (used + 1 >= out_len) return -4;",
      "    }",
      "    if (pfd.revents & POLLHUP) return -5;",
      "  }",
      "  return -6;",
      "}",
      "",
      "static int wait_child(pid_t pid) {",
      "  int status = 0;",
      "  if (waitpid(pid, &status, 0) < 0) return 124;",
      "  if (!WIFEXITED(status)) return 125;",
      "  return WEXITSTATUS(status);",
      "}",
      "",
      "static int close_child(struct child_handle *child) {",
      "  int exit_code;",
      "  if (child->stdin_fd >= 0) close(child->stdin_fd);",
      "  if (child->stdout_fd >= 0) close(child->stdout_fd);",
      "  child->stdin_fd = -1;",
      "  child->stdout_fd = -1;",
      "  exit_code = wait_child(child->pid);",
      "  if (exit_code != 0) { fprintf(stderr, \"child-exit:%d\\n\", exit_code); return 30; }",
      "  return 0;",
      "}",
      "",
      "static int run_protocol_child(int index, int keepalive, struct child_handle *kept) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  char output[512];",
      "  static const char command[] = \"600 URI Acquire\\nURI: probe:/one\\n\\n\";",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    char *argv[] = { (char *)\"/child-protocol-keepalive\", NULL };",
      "    execve(\"/child-protocol-keepalive\", argv, environ);",
      "    perror(\"execve-child\");",
      "    _exit(127);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(out_pipe[1]);",
      "  memset(output, 0, sizeof(output));",
      "  int caps = wait_for_text(out_pipe[0], \"100 Capabilities\", output, sizeof(output));",
      "  if (caps != 0) { fprintf(stderr, \"missing-caps:%d:index=%d:%s\\n\", caps, index, output); return 14; }",
      "  if (write_all(in_pipe[1], command, sizeof(command) - 1) != 0) { perror(\"write-command\"); return 15; }",
      "  memset(output, 0, sizeof(output));",
      "  int done = wait_for_text(out_pipe[0], \"201 URI Done\", output, sizeof(output));",
      "  if (done != 0) { fprintf(stderr, \"missing-done:%d:index=%d:%s\\n\", done, index, output); return 16; }",
      "  if (keepalive) {",
      "    kept->pid = child;",
      "    kept->stdin_fd = in_pipe[1];",
      "    kept->stdout_fd = out_pipe[0];",
      "    return 0;",
      "  }",
      "  close(in_pipe[1]);",
      "  close(out_pipe[0]);",
      "  int exit_code = wait_child(child);",
      "  if (exit_code != 0) { fprintf(stderr, \"child-exit:%d:index=%d\\n\", exit_code, index); return 17; }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  struct child_handle first = { .pid = -1, .stdin_fd = -1, .stdout_fd = -1 };",
      "  int rc = run_protocol_child(1, 1, &first);",
      "  if (rc != 0) return rc;",
      "  rc = run_protocol_child(2, 0, NULL);",
      "  if (first.pid > 0 && close_child(&first) != 0) return 31;",
      "  if (rc != 0) return 100 + rc;",
      "  return write_all(1, \"sequential-fork-protocol-ok\\n\", 28) == 0 ? 0 : 32;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", childOutputPath, childSourcePath], {
      stdio: "pipe",
    });
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", parentOutputPath, parentSourcePath], {
      stdio: "pipe",
    });
    return {
      parent: new Uint8Array(readFileSync(parentOutputPath)),
      child: new Uint8Array(readFileSync(childOutputPath)),
    };
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build sequential fork protocol probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildForkPipeParentExitBeforeChildProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-fork-pipe-parent-exit-before-child-"));
  const sourcePath = join(tmp, "fork_pipe_parent_exit_before_child.c");
  const outputPath = join(tmp, "fork-pipe-parent-exit-before-child");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <poll.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int read_once(int fd, char *buf, size_t cap, size_t *out_len) {",
      "  struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "  int pr = poll(&pfd, 1, 5000);",
      "  if (pr < 0) return -1;",
      "  if (pr == 0 || !(pfd.revents & POLLIN)) return -2;",
      "  ssize_t n = read(fd, buf, cap);",
      "  if (n < 0) return -3;",
      "  *out_len = (size_t)n;",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  int out_pipe[2];",
      "  if (pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(out_pipe[0]);",
      "    close(out_pipe[1]);",
      "    if (write_all(1, \"child-response-before-parent-exit\\n\", 34) != 0) _exit(127);",
      "    sleep(2);",
      "    _exit(0);",
      "  }",
      "  close(out_pipe[1]);",
      "  char buf[128];",
      "  size_t len = 0;",
      "  int got = read_once(out_pipe[0], buf, sizeof(buf), &len);",
      "  if (got != 0) { fprintf(stderr, \"read-child:%d\\n\", got); return 14; }",
      "  if (write_all(1, buf, len) != 0) return 15;",
      "  if (write_all(1, \"parent-exit-before-child:done\\n\", 30) != 0) return 16;",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build fork pipe parent exit-before-child probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildPselectChildExitPipeEofProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-pselect-child-exit-pipe-eof-"));
  const sourcePath = join(tmp, "pselect_child_exit_pipe_eof.c");
  const outputPath = join(tmp, "pselect-child-exit-pipe-eof");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <signal.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/select.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "int main(void) {",
      "  int keep_pipe[2];",
      "  if (pipe(keep_pipe) != 0) { perror(\"keep-pipe\"); return 12; }",
      "  pid_t keepalive = fork();",
      "  if (keepalive < 0) { perror(\"fork-keepalive\"); return 13; }",
      "  if (keepalive == 0) {",
      "    close(keep_pipe[1]);",
      "    char buf[16];",
      "    while (read(keep_pipe[0], buf, sizeof(buf)) > 0) {}",
      "    _exit(0);",
      "  }",
      "  close(keep_pipe[0]);",
      "",
      "  int out_pipe[2];",
      "  if (pipe(out_pipe) != 0) { perror(\"out-pipe\"); return 14; }",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork-child\"); return 15; }",
      "  if (child == 0) {",
      "    close(keep_pipe[1]);",
      "    close(out_pipe[0]);",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(out_pipe[1]);",
      "    usleep(1000);",
      "    _exit(0);",
      "  }",
      "  close(out_pipe[1]);",
      "",
      "  fd_set rfds;",
      "  FD_ZERO(&rfds);",
      "  FD_SET(out_pipe[0], &rfds);",
      "  int ready = pselect(out_pipe[0] + 1, &rfds, NULL, NULL, NULL, NULL);",
      "  if (ready < 0) { perror(\"pselect\"); return 16; }",
      "  if (ready != 1 || !FD_ISSET(out_pipe[0], &rfds)) {",
      "    fprintf(stderr, \"unexpected-pselect-ready:%d\\n\", ready);",
      "    return 17;",
      "  }",
      "  char byte = 0;",
      "  ssize_t n = read(out_pipe[0], &byte, 1);",
      "  if (n != 0) {",
      "    fprintf(stderr, \"unexpected-pipe-read:%zd:%d\\n\", n, (int)byte);",
      "    return 18;",
      "  }",
      "  close(out_pipe[0]);",
      "",
      "  int status = 0;",
      "  if (waitpid(child, &status, 0) < 0) { perror(\"wait-child\"); return 19; }",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "    fprintf(stderr, \"child-status:%d\\n\", status);",
      "    return 20;",
      "  }",
      "  close(keep_pipe[1]);",
      "  if (waitpid(keepalive, &status, 0) < 0) { perror(\"wait-keepalive\"); return 21; }",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "    fprintf(stderr, \"keepalive-status:%d\\n\", status);",
      "    return 22;",
      "  }",
      "  fputs(\"pselect-child-exit-pipe-eof-ok\\n\", stdout);",
      "  return 0;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build pselect child-exit pipe EOF probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildNestedForkWaitPipeResponseProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-nested-fork-wait-pipe-response-"));
  const sourcePath = join(tmp, "nested_fork_wait_pipe_response.c");
  const outputPath = join(tmp, "nested-fork-wait-pipe-response");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <poll.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int wait_for_text(int fd, const char *needle, char *out, size_t out_len) {",
      "  size_t used = 0;",
      "  if (out_len == 0) return -7;",
      "  out[0] = '\\0';",
      "  for (int iter = 0; iter < 400; iter++) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 25);",
      "    if (pr < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    if (pr == 0) continue;",
      "    if (pfd.revents & POLLIN) {",
      "      ssize_t n = read(fd, out + used, out_len - used - 1);",
      "      if (n < 0) {",
      "        if (errno == EINTR) continue;",
      "        return -2;",
      "      }",
      "      if (n == 0) return -3;",
      "      used += (size_t)n;",
      "      out[used] = '\\0';",
      "      if (strstr(out, needle) != NULL) return 0;",
      "      if (used + 1 >= out_len) return -4;",
      "    }",
      "    if (pfd.revents & POLLHUP) return -5;",
      "  }",
      "  return -6;",
      "}",
      "",
      "int main(void) {",
      "  int out_pipe[2];",
      "  if (pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  pid_t child = fork();",
      "  if (child < 0) { perror(\"fork-child\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(out_pipe[0]);",
      "    close(out_pipe[1]);",
      "    if (write_all(1, \"nested-child-ready\\n\", 19) != 0) _exit(127);",
      "    pid_t grandchild = fork();",
      "    if (grandchild < 0) { perror(\"fork-grandchild\"); _exit(121); }",
      "    if (grandchild == 0) _exit(123);",
      "    int status = 0;",
      "    if (waitpid(grandchild, &status, 0) < 0) { perror(\"wait-grandchild\"); _exit(122); }",
      "    if (!WIFEXITED(status) || WEXITSTATUS(status) != 123) _exit(124);",
      "    if (write_all(1, \"nested-child-response-after-wait\\n\", 33) != 0) _exit(125);",
      "    _exit(0);",
      "  }",
      "  close(out_pipe[1]);",
      "  char output[256];",
      "  int response = wait_for_text(out_pipe[0], \"nested-child-response-after-wait\", output, sizeof(output));",
      "  close(out_pipe[0]);",
      "  int status = 0;",
      "  waitpid(child, &status, 0);",
      "  if (response != 0) { fprintf(stderr, \"missing-response:%d:status=%d:%s\\n\", response, status, output); return 14; }",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) { fprintf(stderr, \"child-status:%d\\n\", status); return 15; }",
      "  return write_all(1, \"nested-fork-wait-pipe-response-ok\\n\", 34) == 0 ? 0 : 16;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build nested fork wait pipe response probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildLiveSiblingMemfsGrandchildReadProbe(): Uint8Array | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-live-sibling-memfs-grandchild-read-"));
  const sourcePath = join(tmp, "live_sibling_memfs_grandchild_read.c");
  const outputPath = join(tmp, "live-sibling-memfs-grandchild-read");
  try {
    writeFileSync(sourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <poll.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int wait_for_text(int fd, const char *needle, char *out, size_t out_len) {",
      "  size_t used = 0;",
      "  if (out_len == 0) return -7;",
      "  out[0] = '\\0';",
      "  for (int iter = 0; iter < 400; iter++) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 25);",
      "    if (pr < 0) { if (errno == EINTR) continue; return -1; }",
      "    if (pr == 0) continue;",
      "    if (pfd.revents & POLLIN) {",
      "      ssize_t n = read(fd, out + used, out_len - used - 1);",
      "      if (n < 0) { if (errno == EINTR) continue; return -2; }",
      "      if (n == 0) return -3;",
      "      used += (size_t)n;",
      "      out[used] = '\\0';",
      "      if (strstr(out, needle) != NULL) return 0;",
      "      if (used + 1 >= out_len) return -4;",
      "    }",
      "    if (pfd.revents & POLLHUP) return -5;",
      "  }",
      "  return -6;",
      "}",
      "",
      "static int read_expected_file(const char *label) {",
      "  int fd = open(\"/live-sibling-data\", O_RDONLY);",
      "  if (fd < 0) return 40;",
      "  char buf[4096];",
      "  size_t total = 0;",
      "  for (;;) {",
      "    ssize_t n = read(fd, buf, sizeof(buf));",
      "    if (n < 0) { if (errno == EINTR) continue; close(fd); return 41; }",
      "    if (n == 0) break;",
      "    for (ssize_t i = 0; i < n; i++) {",
      "      char expected = (char)('A' + ((total + (size_t)i) / 4096) % 26);",
      "      if (buf[i] != expected) {",
      "        char msg[96];",
      "        int m = snprintf(msg, sizeof(msg), \"%s-mismatch:%lu:%d:%d\\n\", label, (unsigned long)(total + (size_t)i), (int)(unsigned char)buf[i], (int)(unsigned char)expected);",
      "        if (m > 0) (void)write_all(1, msg, (size_t)m);",
      "        close(fd);",
      "        return 42;",
      "      }",
      "    }",
      "    total += (size_t)n;",
      "  }",
      "  close(fd);",
      "  return total == 131072 ? 0 : 43;",
      "}",
      "",
      "int main(void) {",
      "  int keep_pipe[2];",
      "  int out_pipe[2];",
      "  if (pipe(keep_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  pid_t keepalive = fork();",
      "  if (keepalive < 0) { perror(\"fork-keepalive\"); return 13; }",
      "  if (keepalive == 0) {",
      "    close(keep_pipe[1]);",
      "    char byte;",
      "    while (read(keep_pipe[0], &byte, 1) < 0 && errno == EINTR) {}",
      "    _exit(0);",
      "  }",
      "  close(keep_pipe[0]);",
      "  pid_t worker = fork();",
      "  if (worker < 0) { perror(\"fork-worker\"); return 14; }",
      "  if (worker == 0) {",
      "    if (dup2(out_pipe[1], 1) < 0) _exit(120);",
      "    close(out_pipe[0]);",
      "    close(out_pipe[1]);",
      "    int fd = open(\"/live-sibling-data\", O_CREAT | O_TRUNC | O_WRONLY, 0644);",
      "    if (fd < 0) _exit(121);",
      "    char chunk[4096];",
      "    for (int block = 0; block < 32; block++) {",
      "      for (size_t i = 0; i < sizeof(chunk); i++) chunk[i] = (char)('A' + (block % 26));",
      "      if (write_all(fd, chunk, sizeof(chunk)) != 0) _exit(122);",
      "      off_t off = lseek(fd, 0, SEEK_CUR);",
      "      if (off != (off_t)((block + 1) * (int)sizeof(chunk))) {",
      "        char msg[96];",
      "        int n = snprintf(msg, sizeof(msg), \"worker-offset-mismatch:%d:%ld\\n\", block, (long)off);",
      "        if (n > 0) (void)write_all(1, msg, (size_t)n);",
      "        _exit(129);",
      "      }",
      "    }",
      "    int before_close_check = read_expected_file(\"worker-before-close\");",
      "    if (before_close_check != 0) {",
      "      char msg[80];",
      "      int n = snprintf(msg, sizeof(msg), \"worker-before-close-status:%d\\n\", before_close_check);",
      "      if (n > 0) (void)write_all(1, msg, (size_t)n);",
      "      _exit(127);",
      "    }",
      "    close(fd);",
      "    int self_check = read_expected_file(\"worker-self\");",
      "    if (self_check != 0) {",
      "      char msg[64];",
      "      int n = snprintf(msg, sizeof(msg), \"worker-self-status:%d\\n\", self_check);",
      "      if (n > 0) (void)write_all(1, msg, (size_t)n);",
      "      _exit(128);",
      "    }",
      "    pid_t reader = fork();",
      "    if (reader < 0) _exit(123);",
      "    if (reader == 0) _exit(read_expected_file(\"reader\"));",
      "    int status = 0;",
      "    if (waitpid(reader, &status, 0) < 0) _exit(124);",
      "    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "      char msg[64];",
      "      int n = snprintf(msg, sizeof(msg), \"reader-status:%d\\n\", status);",
      "      if (n > 0) (void)write_all(1, msg, (size_t)n);",
      "      _exit(125);",
      "    }",
      "    if (write_all(1, \"live-sibling-memfs-grandchild-read-ok\\n\", 38) != 0) _exit(126);",
      "    _exit(0);",
      "  }",
      "  close(out_pipe[1]);",
      "  char output[512];",
      "  int got = wait_for_text(out_pipe[0], \"live-sibling-memfs-grandchild-read-ok\", output, sizeof(output));",
      "  if (got != 0) return 15;",
      "  close(out_pipe[0]);",
      "  close(keep_pipe[1]);",
      "  int worker_status = 0;",
      "  int keep_status = 0;",
      "  waitpid(worker, &worker_status, 0);",
      "  waitpid(keepalive, &keep_status, 0);",
      "  if (!WIFEXITED(worker_status) || WEXITSTATUS(worker_status) != 0) return 16;",
      "  return write_all(1, \"live-sibling-memfs-grandchild-read-parent-ok\\n\", 45) == 0 ? 0 : 17;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", outputPath, sourcePath], {
      stdio: "pipe",
    });
    return new Uint8Array(readFileSync(outputPath));
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build live sibling memfs grandchild read probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

function buildVforkPipeDup2ExecProtocolProbe(): { parent: Uint8Array; child: Uint8Array } | null {
  const tmp = mkdtempSync(join(tmpdir(), "tidemark-vfork-pipe-dup2-protocol-"));
  const parentSourcePath = join(tmp, "vfork_pipe_dup2_exec_protocol.c");
  const childSourcePath = join(tmp, "child_protocol_method.c");
  const parentOutputPath = join(tmp, "vfork-pipe-dup2-exec-protocol");
  const childOutputPath = join(tmp, "child-protocol-method");
  try {
    writeFileSync(childSourcePath, [
      "#include <errno.h>",
      "#include <stddef.h>",
      "#include <string.h>",
      "#include <unistd.h>",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "int main(void) {",
      "  static const char caps[] = \"100 Capabilities\\nVersion: 1.2\\nPipeline: true\\n\\n\";",
      "  static const char done[] = \"201 URI Done\\nFilename: /tmp/probe\\nSize: 4\\n\\n\";",
      "  char buf[257];",
      "  size_t total = 0;",
      "  int saw_end = 0;",
      "  if (write_all(1, caps, sizeof(caps) - 1) != 0) return 21;",
      "  while (!saw_end) {",
      "    ssize_t n = read(0, buf, sizeof(buf) - 1);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return 22;",
      "    }",
      "    if (n == 0) return 23;",
      "    buf[n] = '\\0';",
      "    total += (size_t)n;",
      "    if (strstr(buf, \"\\nEND\\n\") != NULL) saw_end = 1;",
      "    if (total > 20000) return 24;",
      "  }",
      "  if (total < 7000) return 25;",
      "  return write_all(1, done, sizeof(done) - 1) == 0 ? 0 : 26;",
      "}",
      "",
    ].join("\n"));
    writeFileSync(parentSourcePath, [
      "#include <errno.h>",
      "#include <fcntl.h>",
      "#include <poll.h>",
      "#include <stdio.h>",
      "#include <string.h>",
      "#include <sys/wait.h>",
      "#include <unistd.h>",
      "",
      "extern char **environ;",
      "",
      "static int write_all(int fd, const char *data, size_t len) {",
      "  size_t off = 0;",
      "  while (off < len) {",
      "    ssize_t n = write(fd, data + off, len - off);",
      "    if (n < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    off += (size_t)n;",
      "  }",
      "  return 0;",
      "}",
      "",
      "static int wait_for_text(int fd, const char *needle, char *out, size_t out_len) {",
      "  size_t used = 0;",
      "  for (int iter = 0; iter < 200; iter++) {",
      "    struct pollfd pfd = { .fd = fd, .events = POLLIN | POLLHUP, .revents = 0 };",
      "    int pr = poll(&pfd, 1, 50);",
      "    if (pr < 0) {",
      "      if (errno == EINTR) continue;",
      "      return -1;",
      "    }",
      "    if (pr == 0) continue;",
      "    if (pfd.revents & POLLIN) {",
      "      ssize_t n = read(fd, out + used, out_len - used - 1);",
      "      if (n < 0) {",
      "        if (errno == EINTR) continue;",
      "        return -2;",
      "      }",
      "      if (n == 0) return -3;",
      "      used += (size_t)n;",
      "      out[used] = '\\0';",
      "      if (strstr(out, needle) != NULL) return 0;",
      "      if (used + 1 >= out_len) return -4;",
      "    }",
      "    if (pfd.revents & POLLHUP) return -5;",
      "  }",
      "  return -6;",
      "}",
      "",
      "int main(void) {",
      "  int in_pipe[2];",
      "  int out_pipe[2];",
      "  char output[1024];",
      "  char payload[8192];",
      "  memset(payload, 'x', sizeof(payload));",
      "  memcpy(payload, \"600 URI Acquire\\nURI: http://example.invalid/probe\\n\\n\", 50);",
      "  memcpy(payload + 7400, \"\\nEND\\n\", 5);",
      "  if (pipe(in_pipe) != 0 || pipe(out_pipe) != 0) { perror(\"pipe\"); return 12; }",
      "  fcntl(in_pipe[0], F_SETFD, FD_CLOEXEC);",
      "  fcntl(out_pipe[1], F_SETFD, FD_CLOEXEC);",
      "  pid_t child = vfork();",
      "  if (child < 0) { perror(\"vfork\"); return 13; }",
      "  if (child == 0) {",
      "    if (dup2(in_pipe[0], 0) < 0) { perror(\"dup2-stdin\"); _exit(125); }",
      "    if (dup2(out_pipe[1], 1) < 0) { perror(\"dup2-stdout\"); _exit(126); }",
      "    close(in_pipe[0]); close(in_pipe[1]);",
      "    close(out_pipe[0]); close(out_pipe[1]);",
      "    (void)open(\"/dev/null\", O_WRONLY);",
      "    char *argv[] = { (char *)\"/child-protocol-method\", NULL };",
      "    execve(\"/child-protocol-method\", argv, environ);",
      "    perror(\"execve-child\");",
      "    _exit(127);",
      "  }",
      "  close(in_pipe[0]);",
      "  close(out_pipe[1]);",
      "  memset(output, 0, sizeof(output));",
      "  int caps = wait_for_text(out_pipe[0], \"100 Capabilities\", output, sizeof(output));",
      "  if (caps != 0) { fprintf(stderr, \"missing-caps:%d:%s\\n\", caps, output); return 14; }",
      "  if (write_all(in_pipe[1], payload, sizeof(payload)) != 0) { perror(\"write-payload\"); return 15; }",
      "  memset(output, 0, sizeof(output));",
      "  int done = wait_for_text(out_pipe[0], \"201 URI Done\", output, sizeof(output));",
      "  close(in_pipe[1]);",
      "  close(out_pipe[0]);",
      "  int status = 0;",
      "  waitpid(child, &status, 0);",
      "  if (done != 0) { fprintf(stderr, \"missing-done:%d:%s:status=%d\\n\", done, output, status); return 16; }",
      "  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {",
      "    fprintf(stderr, \"child-status:%d\\n\", status);",
      "    return 17;",
      "  }",
      "  return write_all(1, \"vfork-protocol-ok\\n\", 18) == 0 ? 0 : 18;",
      "}",
      "",
    ].join("\n"));
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", childOutputPath, childSourcePath], {
      stdio: "pipe",
    });
    execFileSync(hostRiscvGcc, ["-static", "-O2", "-o", parentOutputPath, parentSourcePath], {
      stdio: "pipe",
    });
    return {
      parent: new Uint8Array(readFileSync(parentOutputPath)),
      child: new Uint8Array(readFileSync(childOutputPath)),
    };
  } catch (error) {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    return logSkip(`failed to build vfork pipe dup2 protocol probe with ${hostRiscvGcc}: ${detail}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

test("plain fork preserves inherited pipe write ends before parent close", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildForkPipeDup2Probe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/fork-pipe-dup2"],
      ["LC_ALL=C", "LANG=C"],
      15_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "fork pipe dup2 probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
  } finally {
    runtime.destroy();
  }
}, 25_000);

test("fork dup2 fd bindings survive child execve", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildForkPipeDup2ExecProbe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.writeFile("/child-writer", probeBytes.child, 0o755);
    const result = await spawnWithOutput(
      runtime,
      probeBytes.parent,
      ["/fork-pipe-dup2-exec"],
      ["LC_ALL=C", "LANG=C"],
      20_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "fork pipe dup2 exec probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
  } finally {
    runtime.destroy();
  }
}, 30_000);

test("vfork dup2 fd bindings survive execve for bidirectional protocol pipes", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildVforkPipeDup2ExecProtocolProbe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.writeFile("/child-protocol-method", probeBytes.child, 0o755);
    const result = await spawnWithOutput(
      runtime,
      probeBytes.parent,
      ["/vfork-pipe-dup2-exec-protocol"],
      ["LC_ALL=C", "LANG=C"],
      25_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "vfork pipe dup2 protocol probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toBe("vfork-protocol-ok\n");
  } finally {
    runtime.destroy();
  }
}, 35_000);

test("fork exec child observes stdin EOF after parent closes inherited pipe writer", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildForkPipeDup2ExecStdinEofProbe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.writeFile("/child-read-until-eof", probeBytes.child, 0o755);
    const result = await spawnWithOutput(
      runtime,
      probeBytes.parent,
      ["/fork-pipe-dup2-exec-eof"],
      ["LC_ALL=C", "LANG=C"],
      20_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "fork pipe dup2 exec EOF probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
  } finally {
    runtime.destroy();
  }
}, 30_000);

test("visible parent stdout survives exit while pipe child is still live", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildForkPipeParentExitBeforeChildProbe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/fork-pipe-parent-exit-before-child"],
      ["LC_ALL=C", "LANG=C"],
      15_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "fork pipe parent exit-before-child probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toBe("child-response-before-parent-exit\nparent-exit-before-child:done\n");
  } finally {
    runtime.destroy();
  }
}, 25_000);

test("pselect wakes on child pipe EOF while another child is live", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildPselectChildExitPipeEofProbe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/pselect-child-exit-pipe-eof"],
      ["LC_ALL=C", "LANG=C"],
      15_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "pselect child-exit pipe EOF probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toBe("pselect-child-exit-pipe-eof-ok\n");
  } finally {
    runtime.destroy();
  }
}, 25_000);

test("child pipe response survives nested fork wait", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildNestedForkWaitPipeResponseProbe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/nested-fork-wait-pipe-response"],
      ["LC_ALL=C", "LANG=C"],
      20_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "nested fork wait pipe response probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toBe("nested-fork-wait-pipe-response-ok\n");
  } finally {
    runtime.destroy();
  }
}, 30_000);

test("memfs writes remain visible to a grandchild while a previous child is live", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildLiveSiblingMemfsGrandchildReadProbe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    const result = await spawnWithOutput(
      runtime,
      probeBytes,
      ["/live-sibling-memfs-grandchild-read"],
      ["LC_ALL=C", "LANG=C"],
      25_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "live sibling memfs grandchild read probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toBe("live-sibling-memfs-grandchild-read-parent-ok\n");
  } finally {
    runtime.destroy();
  }
}, 35_000);

test("sequential fork exec protocol pipes survive a live previous child", async () => {
  const wasmBytes = readKernelWasm();
  if (!wasmBytes) {
    return;
  }
  const probeBytes = buildSequentialForkExecProtocolProbe();
  if (!probeBytes) {
    return;
  }

  const runtime = await Runtime.create({ wasmBytes });
  try {
    await runtime.writeFile("/child-protocol-keepalive", probeBytes.child, 0o755);
    const result = await spawnWithOutput(
      runtime,
      probeBytes.parent,
      ["/sequential-fork-protocol"],
      ["LC_ALL=C", "LANG=C"],
      30_000,
      { cwd: "/", stdin: "closed" },
    ).catch(async (error) => {
      const summary = await runtimeDebugSummary(runtime);
      const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
      throw new Error([detail, summary].join("\n"));
    });
    expect(
      result.exitCode,
      [
        "sequential fork protocol probe exit mismatch",
        `exit=${result.exitCode}`,
        `output=${JSON.stringify(result.output)}`,
        `debug=${result.debugInfo ?? ""}`,
      ].join("\n"),
    ).toBe(0);
    expect(result.output).toBe("sequential-fork-protocol-ok\n");
  } finally {
    runtime.destroy();
  }
}, 40_000);
