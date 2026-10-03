// A real assembled lane for tests: grammar events → assembler → deferred sink queue → on-disk
// journal. Assertions read the journal back, so they check what a client sees.

import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { agentJournalItemKey } from '../../../shared/agent-session-journal-item-key'
import type {
  AgentJournalApprovalItem,
  AgentJournalItemBody,
  AgentJournalMessageItem,
  AgentJournalRenderItem,
  AgentJournalToolCallItem,
  AgentJournalTurnLifecycle
} from '../../../shared/agent-session-journal-types'
import { readAgentJournalTurn } from '../../../shared/agent-session-turn-record'
import { createTrackedJournalOpener } from '../agent-session-journal/journal-host-database-test-support'
import type { AgentSessionJournal } from '../agent-session-journal/journal-store'
import {
  createDeferredStructuredAgentSessionEventSink,
  type StructuredAgentSessionEventSink
} from '../agent-session-wire/structured-agent-session-event-sink'
import { testEventSinkLogging } from '../agent-session-wire/structured-agent-session-logger-test-support'
import {
  createProviderTimelineAssembler,
  type ProviderTimelineAssembler,
  type ProviderTimelineAssemblerDeps
} from './provider-timeline-assembler'
import {
  createLegacyProviderTimelineIdentityScheme,
  type ProviderTimelineItemFamily
} from './provider-timeline-identity'
import { providerTimelineSink, type ProviderTimelineSink } from './provider-timeline-plan'

export const SESSION = 'session-timeline'
export const AGENT = 'grok'
export const GENERATION = 'gen-1'
export const NAMESPACE = 'provider-session-1'

const scheme = createLegacyProviderTimelineIdentityScheme({ agent: AGENT, sessionId: SESSION })

/** The journal key the assembler gives a provider-keyed item. */
export function providerItemId(
  family: ProviderTimelineItemFamily,
  key: string,
  options: { namespace?: string; incarnation?: number } = {}
): string {
  return agentJournalItemKey(
    scheme.item({
      namespace: options.namespace ?? NAMESPACE,
      family,
      key: { source: 'provider', value: key },
      thread: null,
      turn: null,
      itemClass: 'message',
      messageOrdinal: null,
      incarnation: options.incarnation ?? 1
    })
  )
}

/** The journal key of a provider-keyed turn's row. */
export function providerTurnItemId(turnKey: string, namespace = NAMESPACE): string {
  return agentJournalItemKey(
    scheme.turn({ namespace, key: { source: 'provider', value: turnKey } })
  )
}

/** The turn id a provider-keyed turn's row carries. */
export function providerTurnId(turnKey: string, namespace = NAMESPACE): string {
  return scheme.turnId({ namespace, key: { source: 'provider', value: turnKey } })
}

export function runningTool(name: string): AgentJournalToolCallItem {
  return { kind: 'tool-call', name, input: { name }, state: 'running' }
}

export function assistantText(text: string): AgentJournalMessageItem {
  return { kind: 'message', role: 'assistant', blocks: [{ type: 'text', text }] }
}

export const pendingApproval: AgentJournalApprovalItem = {
  kind: 'approval',
  title: 'Run?',
  detail: null,
  options: [{ id: 'allow', label: 'Allow' }],
  resolution: { state: 'pending', selectedOptionId: null, resolvedBy: null, resolvedAt: null }
}

export function messageText(body: AgentJournalItemBody | undefined): string | undefined {
  return body?.kind === 'message' && body.blocks[0]?.type === 'text'
    ? body.blocks[0].text
    : undefined
}

const journals = createTrackedJournalOpener()
const cleanups: (() => Promise<void>)[] = []

/** Call from `afterEach`. */
export async function closeProviderTimelineRigs(): Promise<void> {
  for (const cleanup of cleanups.splice(0)) {
    await cleanup()
  }
  await journals.closeAll()
}

export type ProviderTimelineRig = {
  journal: AgentSessionJournal
  assembler: ProviderTimelineAssembler
  sink: ProviderTimelineSink
  /** The same journal's event sink, for a lane that writes it directly. */
  eventSink: StructuredAgentSessionEventSink
  /** A fresh assembler on the same journal, as a restarted host builds. */
  restart(overrides?: Partial<ProviderTimelineAssemblerDeps>): ProviderTimelineAssembler
  rows(): Promise<AgentJournalRenderItem[]>
  row(itemId: string): Promise<AgentJournalRenderItem | undefined>
  /** The turn row of provider turn `turnKey`, or the row whose turn id is `turnKey`. */
  turn(turnKey: string, namespace?: string): Promise<AgentJournalTurnLifecycle | undefined>
  turns(): Promise<AgentJournalTurnLifecycle[]>
}

/** The rig's sink with its transitions refused while `refusing()` holds. */
export function refusingSink(
  sink: ProviderTimelineSink,
  refusing: () => boolean,
  reason: 'backpressure' | 'failed' = 'backpressure'
): ProviderTimelineSink {
  return {
    ...sink,
    tryAppendTransition: (transition) =>
      refusing() ? { accepted: false, reason } : sink.tryAppendTransition(transition)
  }
}

export async function openProviderTimelineRig(
  overrides: Partial<ProviderTimelineAssemblerDeps> = {}
): Promise<ProviderTimelineRig> {
  const root = await mkdtemp(join(tmpdir(), 'orca-provider-timeline-'))
  const journal = await journals.open({
    identity: {
      sessionId: SESSION,
      workspaceId: 'workspace-1',
      hostId: 'local',
      agent: AGENT,
      providerHandle: { kind: 'opaque', agent: AGENT, value: 'provider-session-1' }
    },
    stateDirectory: root,
    now: () => 1_000
  })
  const deferred = createDeferredStructuredAgentSessionEventSink(testEventSinkLogging())
  deferred.bind({ journal, fence: 1, publish: () => {} })
  cleanups.push(async () => {
    deferred.close()
    await rm(root, { recursive: true, force: true })
  })
  const sink = providerTimelineSink(deferred.sink)
  if (!sink) {
    throw new Error('the deferred sink offers transitions')
  }
  const build = (more: Partial<ProviderTimelineAssemblerDeps> = {}) =>
    createProviderTimelineAssembler({
      sink,
      sessionId: SESSION,
      agent: AGENT,
      generation: GENERATION,
      namespace: NAMESPACE,
      // Every delta writes at once unless a test drives the window itself.
      schedule: (run) => {
        run()
        return () => {}
      },
      ...overrides,
      ...more
    })
  const rows = async () => {
    await deferred.drained()
    return journal.snapshot().items
  }
  const turns = async () =>
    (await rows()).flatMap((item) => {
      const turn = readAgentJournalTurn(item.body)
      return turn ? [turn] : []
    })
  return {
    journal,
    assembler: build(),
    sink,
    eventSink: deferred.sink,
    restart: build,
    rows,
    row: async (itemId) => (await rows()).find((item) => item.itemId === itemId),
    turns,
    turn: async (turnKey, namespace) => {
      const all = await turns()
      return (
        all.find((turn) => turn.turnId === providerTurnId(turnKey, namespace)) ??
        all.find((turn) => turn.turnId === turnKey)
      )
    }
  }
}
