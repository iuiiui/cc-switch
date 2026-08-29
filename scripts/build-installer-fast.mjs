import { spawn } from "node:child_process";
import process from "node:process";

const startedAt = Date.now();
const isWindows = process.platform === "win32";

// Keep the normal `pnpm build` lane byte-for-byte compatible with upstream's
// size-optimized release profile. This fast lane overrides the profile only for
// local NSIS packaging, so routine testing does not pay for Thin LTO and a
// single codegen unit after every Rust edit.
const env = {
  ...process.env,
  CARGO_BUILD_JOBS: process.env.CARGO_BUILD_JOBS ?? "2",
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS: "8",
  CARGO_PROFILE_RELEASE_INCREMENTAL: "true",
  CARGO_PROFILE_RELEASE_LTO: "false",
  CARGO_PROFILE_RELEASE_OPT_LEVEL: "2",
};

const tauriArgs = [
  "exec",
  "tauri",
  "build",
  "--bundles",
  "nsis",
  "--config",
  "src-tauri/tauri.fast.conf.json",
];
const command = isWindows ? (process.env.ComSpec ?? "cmd.exe") : "pnpm";
const commandArgs = isWindows
  ? ["/d", "/s", "/c", `pnpm ${tauriArgs.join(" ")}`]
  : tauriArgs;

const child = spawn(command, commandArgs, {
  cwd: process.cwd(),
  env,
  stdio: "inherit",
  windowsHide: true,
});

child.on("error", (error) => {
  console.error(`[fast-installer] failed to start: ${error.message}`);
  process.exitCode = 1;
});

child.on("exit", (code, signal) => {
  const elapsedSeconds = ((Date.now() - startedAt) / 1000).toFixed(1);
  if (signal) {
    console.error(
      `[fast-installer] terminated by ${signal} after ${elapsedSeconds}s`,
    );
    process.exitCode = 1;
    return;
  }
  if (code !== 0) {
    console.error(
      `[fast-installer] failed with exit code ${code} after ${elapsedSeconds}s`,
    );
    process.exitCode = code ?? 1;
    return;
  }
  console.log(`[fast-installer] completed in ${elapsedSeconds}s`);
});
