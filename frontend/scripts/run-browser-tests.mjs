import { spawnSync } from "node:child_process";
import {
  lstatSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  rmSync,
} from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const frontend = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const runsRoot = path.resolve(frontend, "..", ".test_runs", "runs");
const args = process.argv.slice(2);

function outputArgument() {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--output" || arg === "-o") {
      if (!args[index + 1]) throw new Error(`${arg} requires a directory`);
      return args[index + 1];
    }
    if (arg.startsWith("--output=")) return arg.slice("--output=".length);
  }
  return undefined;
}

mkdirSync(runsRoot, { recursive: true });
const requestedOutput = outputArgument();
let ownedRun;
let run;
if (requestedOutput === undefined) {
  ownedRun = mkdtempSync(path.join(runsRoot, "playwright-"));
  run = ownedRun;
  args.push("--output", path.join(run, "results"));
} else {
  const output = path.resolve(frontend, requestedOutput);
  const relative = path.relative(runsRoot, output);
  const segments = relative.split(path.sep);
  if (
    relative.startsWith("..") ||
    path.isAbsolute(relative) ||
    segments.length < 2
  ) {
    throw new Error(`Playwright output must be inside ${runsRoot}/<run-id>/`);
  }
  run = path.join(runsRoot, segments[0]);
  if (!lstatSync(run).isDirectory())
    throw new Error(`Missing run directory: ${run}`);
}

const cli = path.join(
  frontend,
  "node_modules",
  "@playwright",
  "test",
  "cli.js",
);

function removeOwnedRun(directory) {
  if (
    path.dirname(realpathSync(directory)) !== realpathSync(runsRoot) ||
    !lstatSync(directory).isDirectory()
  ) {
    throw new Error(`Unexpected Playwright cleanup target: ${directory}`);
  }
  rmSync(directory, { recursive: true });
}

let result;
try {
  result = spawnSync(process.execPath, [cli, "test", ...args], {
    cwd: frontend,
    stdio: "inherit",
    env: { ...process.env, TEMP: run, TMP: run, TMPDIR: run },
  });
} finally {
  if (ownedRun !== undefined) removeOwnedRun(ownedRun);
}

if (result.error) throw result.error;
process.exitCode = result.status ?? 1;
