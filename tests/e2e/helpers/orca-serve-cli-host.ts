/**
 * `orca serve` through the real CLI (`out/cli/index.js`), so the host it picks is the CLI's own
 * selection: orcad by default, Electron on `ORCA_SERVE_RUNTIME=electron` or a fallback.
 */
import { mkdtempSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { spawnProcess } from '../../../src/shared/child-process/run-process'
import { createElectronHomeIsolation } from './electron-home-isolation'
import { getE2ECompletedOnboardingProfile } from './e2e-completed-onboarding-profile'
import { resolveElectronExecutable } from './daemon-generation-runtime-fixture'

export type CliServe = {
  userDataDir: string
  /** The first stdout line: the chosen host's readiness JSON. */
  readiness: Record<string, unknown>
  stderr: () => string
  stop: () => Promise<void>
}

/** A fresh isolated profile the CLI and whichever host it starts both use. */
export function cliServeProfile(parent: string): { userDataDir: string; env: NodeJS.ProcessEnv } {
  const userDataDir = mkdtempSync(path.join(parent, 'cli-serve-'))
  writeFileSync(
    path.join(userDataDir, 'orca-data.json'),
    `${JSON.stringify(getE2ECompletedOnboardingProfile(), null, 2)}\n`
  )
  const { ELECTRON_RUN_AS_NODE: _unused, ...cleanEnv } = process.env
  void _unused
  const isolation = createElectronHomeIsolation({
    inheritedEnv: cleanEnv,
    launchEnv: { NODE_ENV: 'development', ORCA_E2E_HEADLESS: '1' },
    extraEnv: {},
    userDataDir
  })
  return {
    userDataDir,
    env: {
      ...isolation.env,
      // How `orca serve` from a dev checkout finds its app (config/scripts/orca-dev.mjs).
      ORCA_APP_EXECUTABLE: resolveElectronExecutable(process.cwd()),
      ORCA_APP_EXECUTABLE_NEEDS_APP_ROOT: '1',
      ORCA_USER_DATA_PATH: userDataDir
    }
  }
}

export async function startCliServe(
  profile: { userDataDir: string; env: NodeJS.ProcessEnv },
  extraEnv: NodeJS.ProcessEnv = {}
): Promise<CliServe> {
  const child = spawnProcess({
    program: process.execPath,
    args: [
      path.join(process.cwd(), 'out', 'cli', 'index.js'),
      'serve',
      '--json',
      '--port',
      '0',
      '--pairing-address',
      '127.0.0.1'
    ],
    env: { ...profile.env, ...extraEnv },
    timeoutMs: null
  })
  let stdout = ''
  let stderr = ''
  child.stderr.on('data', (chunk: Buffer) => (stderr += chunk.toString('utf8')))
  // A first run may fetch and verify the pinned Node before orcad starts.
  const line = await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`orca serve not ready: ${stderr}`)), 240_000)
    child.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString('utf8')
      const ready = stdout.split('\n').find((candidate) => candidate.trim().startsWith('{'))
      if (ready) {
        clearTimeout(timer)
        resolve(ready)
      }
    })
    child.once('exit', (code) => {
      clearTimeout(timer)
      reject(new Error(`orca serve exited ${String(code)}: ${stderr}`))
    })
  })
  return {
    userDataDir: profile.userDataDir,
    readiness: JSON.parse(line),
    stderr: () => stderr,
    stop: async () => {
      if (child.exitCode !== null || child.signalCode !== null) {
        return
      }
      const exited = new Promise((settle) => child.once('exit', settle))
      // The CLI forwards SIGTERM to the host it started.
      child.kill('SIGTERM')
      await exited
    }
  }
}
