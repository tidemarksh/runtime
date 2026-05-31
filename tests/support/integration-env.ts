import { existsSync, readFileSync } from "node:fs";
import { isAbsolute, resolve } from "node:path";

type WorkloadArtifactEnvFile = {
  schemaVersion: 1;
  env?: Record<string, unknown>;
};

function isIntegrationRun(): boolean {
  if (process.env.TIDEMARK_TEST_ENABLE_INTEGRATION_ENV === "1") {
    return true;
  }
  const args = process.argv.map((arg) => arg.replaceAll("\\", "/"));
  return args.some((arg) => (
    arg === "tests/integration"
    || arg.startsWith("tests/integration/")
    || arg.endsWith("/tests/integration")
    || arg.includes("/tests/integration/")
  ));
}

function resolveEnvFilePath(): string {
  const configured = process.env.TIDEMARK_TEST_WORKLOAD_ARTIFACT_ENV
    ?? ".artifacts/workload-env.json";
  return isAbsolute(configured) ? configured : resolve(process.cwd(), configured);
}

function applyWorkloadArtifactEnv(): void {
  if (!isIntegrationRun()) {
    return;
  }
  const envFilePath = resolveEnvFilePath();
  if (!existsSync(envFilePath)) {
    return;
  }
  const parsed = JSON.parse(readFileSync(envFilePath, "utf8")) as WorkloadArtifactEnvFile;
  if (parsed.schemaVersion !== 1 || !parsed.env || typeof parsed.env !== "object") {
    throw new Error(`invalid workload artifact env file: ${envFilePath}`);
  }
  for (const [name, value] of Object.entries(parsed.env)) {
    if (typeof value !== "string" || value.length === 0) {
      continue;
    }
    if (!process.env[name]) {
      process.env[name] = value;
    }
  }
}

applyWorkloadArtifactEnv();
