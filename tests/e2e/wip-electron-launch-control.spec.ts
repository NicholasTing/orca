/**
 * WIP control for the Windows D7 crash: the same Electron serve launches and CLI serve launches as
 * orcad-serve-mode-switch.spec.ts, with every orcad leg replaced by a sleep. Never runs orcad.
 */
import { mkdtempSync, rmSync } from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { expect, test } from './helpers/orca-app'
import { launchHeadlessPairedRuntimeHost } from './helpers/headless-paired-runtime-host'
import { cleanupE2EDaemons } from './helpers/electron-process-shutdown'
import { cliServeProfile, startCliServe } from './helpers/orca-serve-cli-host'

const RUN = process.env.ORCA_E2E_LAUNCH_CONTROL === '1'
const ORCAD_LEG_MS = 4_000

test.describe.configure({ mode: 'serial' })

let scratch = ''

test.beforeAll(() => {
  test.skip(!RUN, 'Set ORCA_E2E_LAUNCH_CONTROL=1 to run the launch control.')
  scratch = mkdtempSync(path.join(os.tmpdir(), 'orca-ctl-'))
})

test.afterAll(() => {
  if (scratch) {
    rmSync(scratch, { recursive: true, force: true })
  }
})

const sleep = (ms: number): Promise<void> => new Promise((settle) => setTimeout(settle, ms))

async function hostWithRestarts(restarts: number): Promise<void> {
  const host = await launchHeadlessPairedRuntimeHost({
    pinnedServePort: true,
    userDataParent: scratch
  })
  try {
    for (let i = 0; i < restarts; i += 1) {
      await host.restartServeProcess({ betweenProcesses: () => sleep(ORCAD_LEG_MS) })
    }
  } finally {
    await host.dispose()
  }
}

for (const round of [1, 2, 3]) {
  test(`round ${round}: launch beside a sleep (cross-profile leg)`, async () => {
    await sleep(ORCAD_LEG_MS)
    await hostWithRestarts(0)
  })

  test(`round ${round}: launch, sleep, relaunch (E → orcad → E)`, async () => {
    await hostWithRestarts(1)
  })

  test(`round ${round}: launch, sleep, relaunch (daemon owns)`, async () => {
    await hostWithRestarts(1)
  })

  test(`round ${round}: launch, sleep, relaunch (refuses)`, async () => {
    await hostWithRestarts(1)
  })

  test(`round ${round}: CLI serve on Electron twice`, async () => {
    const profile = cliServeProfile(scratch)
    try {
      for (let i = 0; i < 2; i += 1) {
        const serve = await startCliServe(profile, { ORCA_SERVE_RUNTIME: 'electron' })
        expect(serve.readiness).toBeTruthy()
        await serve.stop()
      }
    } finally {
      await cleanupE2EDaemons(profile.userDataDir)
    }
  })
}
