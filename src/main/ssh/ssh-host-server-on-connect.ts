/**
 * Which server an SSH host runs, decided once per connect before any relay is started.
 *
 * Every host runs managed orcad: an empty host deploys it, a host with Orca state converts through
 * the journaled migration, and a converted host connects through its tunnel, updating first when
 * it runs an older build and its terminals allow. A host whose relay terminals are live, or can't
 * be proven exited, keeps the relay this session and converts on a later connect. A host orcad
 * can't run on keeps the pinned-relay ladder, with the reason recorded so the deploy isn't retried
 * on every connect. A host whose sshd refuses port forwarding reaches its server through the
 * stdio bridge (orcad-managed-tunnel-transport.ts) instead.
 */
import type {
  OrcadManagedConversionResult,
  OrcadManagedDeployResult
} from '../../shared/orcad-managed-runtime'
import type {
  SshManagedServerRelayReason,
  SshManagedServerUpdateNote,
  SshTarget
} from '../../shared/ssh-types'
import type { ManagedOrcadAutoUpdateOutcome } from './orcad-managed-auto-update'
import {
  classifyOrcadHostUnavailable,
  ORCAD_TUNNEL_UNAVAILABLE_REASON
} from './orcad-host-unavailable'

export type HostServerPhase = 'deploying' | 'converting' | 'connecting' | 'updating'

export type HostServerOnConnectResult =
  | { route: 'managed'; environmentId: string; update?: SshManagedServerUpdateNote }
  | {
      route: 'relay'
      reason: SshManagedServerRelayReason
      /** A blocker the user must act on, or why orcad can't run on this host. */
      detail?: string
      terminals?: number
    }

export type HostServerTerminalVerdict = {
  verdict: 'exited' | 'live' | 'unverifiable'
  /** Relay terminals known to be running or unproven. */
  count: number
}

export type HostServerOnConnectDeps = {
  managedEnvironmentId: (target: SshTarget) => string | null
  ensureTunnel: (environmentId: string) => Promise<void>
  /** Retires a retained source once retirement is switched on; a failure only defers it. */
  retireRetainedSource: (target: SshTarget) => Promise<void>
  /** False when this build carries no orcad template, so nothing is tried on the host. */
  hasTemplate: () => boolean
  /** A recorded "orcad can't run here" whose key still matches this build. */
  recordedUnavailable: (target: SshTarget) => string | null
  recordUnavailable: (target: SshTarget, reason: string) => void
  /** True when the host holds no Orca state, so it deploys without a migration. */
  isEmptyHost: (target: SshTarget) => boolean
  relayTerminals: (target: SshTarget) => Promise<HostServerTerminalVerdict>
  deploy: (target: SshTarget) => Promise<OrcadManagedDeployResult>
  convert: (target: SshTarget) => Promise<OrcadManagedConversionResult>
  /** Releases a conversion fence nothing remote holds, so the relay can serve the host. */
  abandonConversion: (target: SshTarget) => Promise<void>
  /** Releases an empty-host deploy claim whose server was never registered. */
  abandonDeploy: (target: SshTarget) => Promise<void>
  progress: (target: SshTarget, phase: HostServerPhase) => void
  /** A conversion fenced the host and registered its server, but staged nothing there yet. */
  isFencedBeforeStaging: (target: SshTarget) => boolean
  /** Unregisters that unreachable server and releases its fence, so the relay serves again. */
  releaseUnreachableSetup: (target: SshTarget) => Promise<void>
  /** Runs the Managed servers update when the host is behind this app; `onUpdating` fires first. */
  autoUpdate: (
    environmentId: string,
    options: { failedBefore: boolean; onUpdating: () => void }
  ) => Promise<ManagedOrcadAutoUpdateOutcome>
  /** Why an update to this app version already failed on the host, so it isn't retried. */
  recordedUpdateFailure: (target: SshTarget) => string | null
  recordUpdateFailure: (target: SshTarget, reason: string) => void
  clearUpdateFailure: (target: SshTarget) => void
}

export async function resolveHostServerOnConnect(
  target: SshTarget,
  deps: HostServerOnConnectDeps
): Promise<HostServerOnConnectResult> {
  if (target.orcadFence?.sourceChangedAt) {
    return { route: 'relay', reason: 'source_changed' }
  }
  const existing = deps.managedEnvironmentId(target)
  if (existing) {
    deps.progress(target, 'connecting')
    try {
      await deps.ensureTunnel(existing)
    } catch (error) {
      // Why only before staging: once the server holds staged state, only it can say what moved.
      if (
        classifyOrcadHostUnavailable(error) === ORCAD_TUNNEL_UNAVAILABLE_REASON &&
        deps.isFencedBeforeStaging(target)
      ) {
        await deps.releaseUnreachableSetup(target)
        deps.recordUnavailable(target, ORCAD_TUNNEL_UNAVAILABLE_REASON)
        return {
          route: 'relay',
          reason: 'orcad_unavailable',
          detail: ORCAD_TUNNEL_UNAVAILABLE_REASON
        }
      }
      throw error
    }
    await deps.retireRetainedSource(target).catch((error: unknown) => {
      console.warn('[ssh] Source retirement deferred to a later connect:', error)
    })
    const update = await updateOnConnect(target, existing, deps)
    return { route: 'managed', environmentId: existing, ...(update ? { update } : {}) }
  }
  const recorded = deps.recordedUnavailable(target)
  if (recorded) {
    return { route: 'relay', reason: 'orcad_unavailable', detail: recorded }
  }
  if (!deps.hasTemplate()) {
    return { route: 'relay', reason: 'orcad_unavailable', detail: 'artifacts_unavailable' }
  }
  const empty = deps.isEmptyHost(target)
  try {
    if (empty) {
      deps.progress(target, 'deploying')
      return await afterDeploy(target, deps, await deps.deploy(target))
    }
    const terminals = await deps.relayTerminals(target)
    if (terminals.verdict !== 'exited') {
      // Why unverifiable too: loss of contact is never evidence that a relay terminal exited.
      return {
        route: 'relay',
        reason: terminalReason(terminals.verdict),
        terminals: terminals.count
      }
    }
    deps.progress(target, 'converting')
    return await afterConversion(target, deps, await deps.convert(target))
  } catch (error) {
    console.warn('[ssh] Managed Orca server setup failed; using the relay this session:', error)
    return unfinished(target, deps, classifyOrcadHostUnavailable(error), empty, 'failed')
  }
}

/** The host keeps serving whatever happens here, so nothing in it can fail the connect. */
async function updateOnConnect(
  target: SshTarget,
  environmentId: string,
  deps: HostServerOnConnectDeps
): Promise<SshManagedServerUpdateNote | undefined> {
  const failure = deps.recordedUpdateFailure(target)
  let result: ManagedOrcadAutoUpdateOutcome
  try {
    result = await deps.autoUpdate(environmentId, {
      failedBefore: failure !== null,
      onUpdating: () => deps.progress(target, 'updating')
    })
  } catch (error) {
    console.warn('[ssh] Could not check the managed Orca server for an update:', error)
    return undefined
  }
  // Why clear on current too: a Managed servers update may have landed the build since.
  if (
    failure !== null &&
    (result.outcome === 'updated' || (result.outcome === 'skipped' && result.reason === 'current'))
  ) {
    deps.clearUpdateFailure(target)
  }
  switch (result.outcome) {
    case 'updated':
      return undefined
    case 'deferred':
      return { state: 'deferred', detail: result.reason }
    case 'failed':
      deps.recordUpdateFailure(target, result.reason)
      return { state: 'failed', detail: result.reason }
    case 'skipped':
      if (result.reason === 'host-newer') {
        return { state: 'host-newer' }
      }
      return result.reason === 'failed-before' && failure
        ? { state: 'failed', detail: failure }
        : undefined
  }
}

function terminalReason(verdict: 'live' | 'unverifiable'): SshManagedServerRelayReason {
  return verdict === 'live' ? 'relay_terminals_live' : 'relay_terminals_unverifiable'
}

async function afterDeploy(
  target: SshTarget,
  deps: HostServerOnConnectDeps,
  result: OrcadManagedDeployResult
): Promise<HostServerOnConnectResult> {
  if (result.outcome === 'deferred') {
    return deferred(target, deps, result.code, true)
  }
  return { route: 'managed', environmentId: result.environment.id }
}

async function afterConversion(
  target: SshTarget,
  deps: HostServerOnConnectDeps,
  result: OrcadManagedConversionResult
): Promise<HostServerOnConnectResult> {
  switch (result.outcome) {
    case 'converted':
      return { route: 'managed', environmentId: result.environment.id }
    case 'deferred':
      return deferred(target, deps, result.code, false)
    case 'refused':
      if (result.code === 'orcad_migration_terminals') {
        return { route: 'relay', reason: terminalReason(result.verdict) }
      }
      return { route: 'relay', reason: 'refused', detail: result.reason }
  }
}

function deferred(
  target: SshTarget,
  deps: HostServerOnConnectDeps,
  code: string,
  empty: boolean
): Promise<HostServerOnConnectResult> {
  return unfinished(target, deps, classifyOrcadHostUnavailable({ code }), empty, 'deferred')
}

/**
 * A setup that stopped short: its claim or fence goes first, since a fenced host refuses the relay
 * and nothing remote serves it yet. A host orcad can't run on also records why.
 */
async function unfinished(
  target: SshTarget,
  deps: HostServerOnConnectDeps,
  unavailable: string | null,
  empty: boolean,
  reason: 'deferred' | 'failed'
): Promise<HostServerOnConnectResult> {
  try {
    await (empty ? deps.abandonDeploy(target) : deps.abandonConversion(target))
  } catch (error) {
    // A staged or registered destination keeps its fence; the connect reports it below.
    console.warn('[ssh] The managed Orca server setup keeps its fence:', error)
  }
  if (!unavailable) {
    return { route: 'relay', reason }
  }
  deps.recordUnavailable(target, unavailable)
  return { route: 'relay', reason: 'orcad_unavailable', detail: unavailable }
}
