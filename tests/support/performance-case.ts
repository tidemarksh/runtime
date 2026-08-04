export const CASE_RESULT_SCHEMA = "execution-case-result-v1";
const MAX_U64 = 18_446_744_073_709_551_615n;
const acceptedResultBrand: unique symbol = Symbol("accepted-execution-case-result");

export type CaseClass = "semantic" | "diagnostic";

export interface ExecutionCase {
  technicalName: string;
  class: CaseClass;
  repositoryOwner: "kernel" | "runtime";
  definitionSourceDigest: string;
  inputDigest: string;
  usefulWorkUnit: string;
  usefulWorkCount: bigint;
  calibratedRepetitionCount: bigint;
  oracleContractDigest: string;
  counterContractDigest: string;
  caseScheduleDigest: string;
}

export interface ExecutionIdentity {
  engine: string;
  configurationDigest: string;
  artifactDigest: string;
}

export interface ExpectedCounter {
  name: string;
  value: bigint;
}

export interface ObservedCounter {
  name: string;
  value: bigint;
}

export interface MetricContract {
  name: string;
  unit: string;
}

export interface ObservedMetric extends MetricContract {
  value: bigint;
}

export interface OperationObservation {
  oracle: string;
  counters: ObservedCounter[];
  metrics?: ObservedMetric[];
}

export interface CounterResult {
  name: string;
  expected: bigint;
  observed: bigint;
}

export interface MetricResult extends ObservedMetric {}

export interface RawCaseResult {
  readonly [acceptedResultBrand]: true;
  case: ExecutionCase;
  execution: ExecutionIdentity;
  elapsedNs: bigint;
  oracleExpected: string;
  oracleObserved: string;
  counters: CounterResult[];
  metrics: MetricResult[];
}

export interface BenchmarkMetricResult extends MetricContract {
  values: bigint[];
}

export interface BenchmarkCaseResult {
  case: ExecutionCase;
  execution: ExecutionIdentity;
  warmupRuns: number;
  measurementRuns: number;
  elapsedNs: bigint[];
  meanUsefulWorkPerSecond: number;
  medianUsefulWorkPerSecond: number;
  minUsefulWorkPerSecond: number;
  maxUsefulWorkPerSecond: number;
  coefficientOfVariation: number;
  metrics: BenchmarkMetricResult[];
}

export function measureCase<Output>(args: {
  expectedCase: ExecutionCase;
  observedCase: ExecutionCase;
  expectedExecution: ExecutionIdentity;
  observedExecution: ExecutionIdentity;
  oracleExpected: string;
  countersExpected: ExpectedCounter[];
  metricsExpected?: MetricContract[];
}, operation: () => Output, observe: (output: Output) => OperationObservation): RawCaseResult {
  const started = process.hrtime.bigint();
  const output = operation();
  const elapsedNs = process.hrtime.bigint() - started;
  const observation = observe(output);
  return acceptCaseResult({ ...args, elapsedNs, observation });
}

export async function measureAsyncCase<Output>(args: {
  expectedCase: ExecutionCase;
  observedCase: ExecutionCase;
  expectedExecution: ExecutionIdentity;
  observedExecution: ExecutionIdentity;
  oracleExpected: string;
  countersExpected: ExpectedCounter[];
  metricsExpected?: MetricContract[];
}, operation: () => Promise<Output>, observe: (output: Output) => OperationObservation): Promise<RawCaseResult> {
  const started = process.hrtime.bigint();
  const output = await operation();
  const elapsedNs = process.hrtime.bigint() - started;
  const observation = observe(output);
  return acceptCaseResult({ ...args, elapsedNs, observation });
}

export function acceptCaseResult(args: {
  expectedCase: ExecutionCase;
  observedCase: ExecutionCase;
  expectedExecution: ExecutionIdentity;
  observedExecution: ExecutionIdentity;
  elapsedNs: bigint;
  oracleExpected: string;
  observation: OperationObservation;
  countersExpected: ExpectedCounter[];
  metricsExpected?: MetricContract[];
}): RawCaseResult {
  validateCase(args.expectedCase);
  validateCase(args.observedCase);
  validateExecutionIdentity(args.expectedExecution);
  validateExecutionIdentity(args.observedExecution);
  if (!sameCase(args.expectedCase, args.observedCase)) {
    throw new Error("case identity drift");
  }
  if (!sameExecution(args.expectedExecution, args.observedExecution)) {
    throw new Error("engine or configuration identity drift");
  }
  if (args.oracleExpected !== args.observation.oracle) {
    throw new Error(
      `oracle mismatch: expected ${JSON.stringify(args.oracleExpected)}, observed ${JSON.stringify(args.observation.oracle)}`,
    );
  }
  validateExpectedCounters(args.countersExpected);
  if (args.countersExpected.length !== args.observation.counters.length) {
    throw new Error(
      `counter count mismatch: expected ${args.countersExpected.length}, observed ${args.observation.counters.length}`,
    );
  }
  const counters = args.countersExpected.map((expected, index) => {
    const observed = args.observation.counters[index];
    if (expected.name !== observed.name || expected.value !== observed.value) {
      throw new Error(
        `counter mismatch for ${JSON.stringify(expected.name)}: expected ${expected.name}=${expected.value}, observed ${observed.name}=${observed.value}`,
      );
    }
    return { name: expected.name, expected: expected.value, observed: observed.value };
  });
  const metricsExpected = args.metricsExpected ?? [];
  const metricsObserved = args.observation.metrics ?? [];
  validateMetricContracts(metricsExpected);
  if (metricsExpected.length !== metricsObserved.length) {
    throw new Error(
      `metric count mismatch: expected ${metricsExpected.length}, observed ${metricsObserved.length}`,
    );
  }
  const metrics = metricsExpected.map((expected, index) => {
    const observed = metricsObserved[index];
    if (expected.name !== observed.name || expected.unit !== observed.unit) {
      throw new Error(
        `metric contract mismatch: expected ${expected.name}/${expected.unit}, observed ${observed.name}/${observed.unit}`,
      );
    }
    if (observed.value < 0n || observed.value > MAX_U64) {
      throw new Error("metric value must fit u64");
    }
    return { ...observed };
  });
  if (args.elapsedNs <= 0n || args.elapsedNs > MAX_U64) {
    throw new Error("elapsed duration must be positive");
  }
  return {
    [acceptedResultBrand]: true,
    case: args.observedCase,
    execution: args.observedExecution,
    elapsedNs: args.elapsedNs,
    oracleExpected: args.oracleExpected,
    oracleObserved: args.observation.oracle,
    counters,
    metrics,
  };
}

export function rawCaseResultJson(result: RawCaseResult): string {
  if (result[acceptedResultBrand] !== true) {
    throw new Error("case result was not accepted");
  }
  return JSON.stringify({
    schema: CASE_RESULT_SCHEMA,
    technical_name: result.case.technicalName,
    class: result.case.class,
    repository_owner: result.case.repositoryOwner,
    definition_source_digest: result.case.definitionSourceDigest,
    input_digest: result.case.inputDigest,
    useful_work_unit: result.case.usefulWorkUnit,
    useful_work_count: result.case.usefulWorkCount.toString(),
    calibrated_repetition_count: result.case.calibratedRepetitionCount.toString(),
    oracle_contract_digest: result.case.oracleContractDigest,
    counter_contract_digest: result.case.counterContractDigest,
    case_schedule_digest: result.case.caseScheduleDigest,
    engine: result.execution.engine,
    configuration_digest: result.execution.configurationDigest,
    artifact_digest: result.execution.artifactDigest,
    elapsed_ns: result.elapsedNs.toString(),
    correctness: "pass",
    oracle_expected: result.oracleExpected,
    oracle_observed: result.oracleObserved,
    counters: result.counters.map(counter => ({
      name: counter.name,
      expected: counter.expected.toString(),
      observed: counter.observed.toString(),
    })),
    ...(result.metrics.length === 0 ? {} : {
      metrics: result.metrics.map(metric => ({
        name: metric.name,
        unit: metric.unit,
        value: metric.value.toString(),
      })),
    }),
  });
}

export function benchmarkCase<Output>(args: {
  expectedCase: ExecutionCase;
  observedCase: ExecutionCase;
  expectedExecution: ExecutionIdentity;
  observedExecution: ExecutionIdentity;
  oracleExpected: string;
  countersExpected: ExpectedCounter[];
  metricsExpected?: MetricContract[];
  warmupRuns?: number;
  measurementRuns?: number;
}, operation: () => Output, observe: (output: Output) => OperationObservation): BenchmarkCaseResult {
  const warmupRuns = args.warmupRuns ?? 1;
  const measurementRuns = args.measurementRuns ?? 5;
  validateRunCounts(warmupRuns, measurementRuns);
  for (let index = 0; index < warmupRuns; index += 1) {
    measureCase(args, operation, observe);
  }
  const results = Array.from(
    { length: measurementRuns },
    () => measureCase(args, operation, observe),
  );
  return summarizeBenchmarkResults(results, warmupRuns);
}

export async function benchmarkAsyncCase<Output>(args: {
  expectedCase: ExecutionCase;
  observedCase: ExecutionCase;
  expectedExecution: ExecutionIdentity;
  observedExecution: ExecutionIdentity;
  oracleExpected: string;
  countersExpected: ExpectedCounter[];
  metricsExpected?: MetricContract[];
  warmupRuns?: number;
  measurementRuns?: number;
}, operation: () => Promise<Output>, observe: (output: Output) => OperationObservation): Promise<BenchmarkCaseResult> {
  const warmupRuns = args.warmupRuns ?? 1;
  const measurementRuns = args.measurementRuns ?? 5;
  validateRunCounts(warmupRuns, measurementRuns);
  for (let index = 0; index < warmupRuns; index += 1) {
    await measureAsyncCase(args, operation, observe);
  }
  const results: RawCaseResult[] = [];
  for (let index = 0; index < measurementRuns; index += 1) {
    results.push(await measureAsyncCase(args, operation, observe));
  }
  return summarizeBenchmarkResults(results, warmupRuns);
}

export interface InterleavedAsyncBenchmarkCase<Output> {
  args: {
    expectedCase: ExecutionCase;
    observedCase: ExecutionCase;
    expectedExecution: ExecutionIdentity;
    observedExecution: ExecutionIdentity;
    oracleExpected: string;
    countersExpected: ExpectedCounter[];
    metricsExpected?: MetricContract[];
  };
  operation: () => Promise<Output>;
  observe: (output: Output) => OperationObservation;
}

export async function benchmarkInterleavedAsyncCases<Output>(
  cases: readonly InterleavedAsyncBenchmarkCase<Output>[],
  options: { warmupRuns?: number; measurementRuns?: number } = {},
): Promise<BenchmarkCaseResult[]> {
  const warmupRuns = options.warmupRuns ?? 1;
  const measurementRuns = options.measurementRuns ?? 5;
  validateRunCounts(warmupRuns, measurementRuns);
  if (cases.length < 2) {
    throw new Error("interleaved benchmark requires at least two cases");
  }
  const measured = cases.map(() => [] as RawCaseResult[]);
  const runCycle = async (cycle: number, record: boolean): Promise<void> => {
    const indexes = cases.map((_, index) => index);
    if (cycle % 2 === 1) indexes.reverse();
    for (const index of indexes) {
      const entry = cases[index];
      const result = await measureAsyncCase(entry.args, entry.operation, entry.observe);
      if (record) measured[index].push(result);
    }
  };
  for (let cycle = 0; cycle < warmupRuns; cycle += 1) {
    await runCycle(cycle, false);
  }
  for (let cycle = 0; cycle < measurementRuns; cycle += 1) {
    await runCycle(cycle, true);
  }
  return measured.map(results => summarizeBenchmarkResults(results, warmupRuns));
}

export function benchmarkCaseResultJson(result: BenchmarkCaseResult): string {
  return JSON.stringify({
    schema: "execution-benchmark-result-v1",
    technical_name: result.case.technicalName,
    repository_owner: result.case.repositoryOwner,
    protocol_key: [
      result.case.definitionSourceDigest,
      result.case.inputDigest,
      result.case.oracleContractDigest,
      result.case.counterContractDigest,
      result.case.caseScheduleDigest,
      result.case.usefulWorkUnit,
      result.case.usefulWorkCount.toString(),
      result.warmupRuns.toString(),
      result.measurementRuns.toString(),
    ].join(":"),
    useful_work_unit: result.case.usefulWorkUnit,
    useful_work_count: result.case.usefulWorkCount.toString(),
    warmup_runs: result.warmupRuns,
    measurement_runs: result.measurementRuns,
    elapsed_ns: result.elapsedNs.map(value => value.toString()),
    mean_useful_work_per_second: result.meanUsefulWorkPerSecond,
    median_useful_work_per_second: result.medianUsefulWorkPerSecond,
    min_useful_work_per_second: result.minUsefulWorkPerSecond,
    max_useful_work_per_second: result.maxUsefulWorkPerSecond,
    coefficient_of_variation: result.coefficientOfVariation,
    correctness: "pass",
    ...(result.metrics.length === 0 ? {} : {
      metrics: result.metrics.map(metric => ({
        name: metric.name,
        unit: metric.unit,
        values: metric.values.map(value => value.toString()),
      })),
    }),
  });
}

function validateRunCounts(warmupRuns: number, measurementRuns: number): void {
  if (!Number.isSafeInteger(warmupRuns) || warmupRuns < 0) {
    throw new Error("warmup run count must be a non-negative safe integer");
  }
  if (!Number.isSafeInteger(measurementRuns) || measurementRuns < 2) {
    throw new Error("measurement run count must be a safe integer of at least two");
  }
}

function summarizeBenchmarkResults(results: RawCaseResult[], warmupRuns: number): BenchmarkCaseResult {
  const [first] = results;
  if (first === undefined) {
    throw new Error("benchmark requires measured results");
  }
  const rates = results.map(result =>
    Number(result.case.usefulWorkCount) * 1_000_000_000 / Number(result.elapsedNs)
  );
  const mean = rates.reduce((sum, value) => sum + value, 0) / rates.length;
  const sorted = [...rates].sort((left, right) => left - right);
  const midpoint = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 0
    ? (sorted[midpoint - 1] + sorted[midpoint]) / 2
    : sorted[midpoint];
  const sampleVariance = rates.length > 1
    ? rates.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (rates.length - 1)
    : 0;
  return {
    case: first.case,
    execution: first.execution,
    warmupRuns,
    measurementRuns: results.length,
    elapsedNs: results.map(result => result.elapsedNs),
    meanUsefulWorkPerSecond: mean,
    medianUsefulWorkPerSecond: median,
    minUsefulWorkPerSecond: sorted[0],
    maxUsefulWorkPerSecond: sorted[sorted.length - 1],
    coefficientOfVariation: mean === 0 ? 0 : Math.sqrt(sampleVariance) / mean,
    metrics: first.metrics.map((metric, index) => ({
      name: metric.name,
      unit: metric.unit,
      values: results.map(result => result.metrics[index].value),
    })),
  };
}

function validateCase(value: ExecutionCase): void {
  validateTechnicalName(value.technicalName, "technicalName");
  if (value.class !== "semantic" && value.class !== "diagnostic") {
    throw new Error("class must be semantic or diagnostic");
  }
  if (value.repositoryOwner !== "kernel" && value.repositoryOwner !== "runtime") {
    throw new Error("repositoryOwner must be kernel or runtime");
  }
  validateTechnicalName(value.usefulWorkUnit, "usefulWorkUnit");
  if (value.usefulWorkCount <= 0n || value.usefulWorkCount > MAX_U64) {
    throw new Error("usefulWorkCount must be positive");
  }
  if (value.calibratedRepetitionCount <= 0n || value.calibratedRepetitionCount > MAX_U64) {
    throw new Error("calibratedRepetitionCount must be positive");
  }
  for (const [label, digest] of [
    ["definitionSourceDigest", value.definitionSourceDigest],
    ["inputDigest", value.inputDigest],
    ["oracleContractDigest", value.oracleContractDigest],
    ["counterContractDigest", value.counterContractDigest],
    ["caseScheduleDigest", value.caseScheduleDigest],
  ] as const) {
    validateDigest(digest, label);
  }
}

function validateExecutionIdentity(value: ExecutionIdentity): void {
  validateTechnicalName(value.engine, "engine");
  validateDigest(value.configurationDigest, "configurationDigest");
  validateDigest(value.artifactDigest, "artifactDigest");
}

function validateExpectedCounters(counters: ExpectedCounter[]): void {
  if (counters.length === 0) {
    throw new Error("at least one exact counter is required");
  }
  let previous: string | undefined;
  for (const counter of counters) {
    validateTechnicalName(counter.name, "counter name");
    if (counter.value < 0n || counter.value > MAX_U64) {
      throw new Error("counter value must fit u64");
    }
    if (previous !== undefined && previous >= counter.name) {
      throw new Error("counter names must be sorted and unique");
    }
    previous = counter.name;
  }
}

function validateMetricContracts(metrics: MetricContract[]): void {
  let previous: string | undefined;
  for (const metric of metrics) {
    validateTechnicalName(metric.name, "metric name");
    validateTechnicalName(metric.unit, "metric unit");
    if (previous !== undefined && previous >= metric.name) {
      throw new Error("metric names must be sorted and unique");
    }
    previous = metric.name;
  }
}

function validateTechnicalName(value: string, label: string): void {
  if (!/^[a-z0-9_-]+(?:\/[a-z0-9_-]+)*$/.test(value)) {
    throw new Error(`${label} is not a stable technical name: ${JSON.stringify(value)}`);
  }
}

function validateDigest(value: string, label: string): void {
  if (!/^[0-9a-f]{64}$/.test(value)) {
    throw new Error(`${label} must be 64 lowercase hexadecimal characters`);
  }
}

function sameCase(left: ExecutionCase, right: ExecutionCase): boolean {
  return left.technicalName === right.technicalName
    && left.class === right.class
    && left.repositoryOwner === right.repositoryOwner
    && left.definitionSourceDigest === right.definitionSourceDigest
    && left.inputDigest === right.inputDigest
    && left.usefulWorkUnit === right.usefulWorkUnit
    && left.usefulWorkCount === right.usefulWorkCount
    && left.calibratedRepetitionCount === right.calibratedRepetitionCount
    && left.oracleContractDigest === right.oracleContractDigest
    && left.counterContractDigest === right.counterContractDigest
    && left.caseScheduleDigest === right.caseScheduleDigest;
}

function sameExecution(left: ExecutionIdentity, right: ExecutionIdentity): boolean {
  return left.engine === right.engine
    && left.configurationDigest === right.configurationDigest
    && left.artifactDigest === right.artifactDigest;
}
