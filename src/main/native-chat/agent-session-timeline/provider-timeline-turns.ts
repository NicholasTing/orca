// The session's open turn, and the row that records it.
//
// Memory holds which turn is open (the scope rows join, and the id a client's Stop names) and the
// turns this assembler wrote. Whether a turn row exists, and whether it is still running, is the
// journal's answer: a turn this assembler never saw is asked of the journal before it is treated
// as new, and every turn-row write lands only while the journal still agrees.

import {
  agentJournalItemKey,
  agentJournalSubmissionKey,
  parseAgentJournalItemKey
} from '../../../shared/agent-session-journal-item-key'
import {
  AGENT_JOURNAL_THREAD_SCOPE,
  type AgentJournalTurnLifecycle,
  type AgentJournalTurnScope
} from '../../../shared/agent-session-journal-types'
import {
  agentJournalTurnBody,
  readAgentJournalTurn
} from '../../../shared/agent-session-turn-record'
import { estimateStructuredAgentSessionItemBytes } from '../agent-session-wire/structured-agent-session-event-sink-estimate'
import type { StructuredAgentSessionTransitionJournal } from '../agent-session-wire/structured-agent-session-transition'
import {
  agentJournalTurnRowReservedBytes,
  resolveAgentJournalTurnRowWrite
} from './agent-journal-turn-row-revision'
import type {
  ProviderTimelineAddresses,
  ProviderTimelineTurnRef
} from './provider-timeline-addresses'
import type { ProviderTimelinePlan } from './provider-timeline-plan'

/** Turns this assembler wrote, remembered so it need not ask the journal about its own. */
const MAX_KNOWN_TURNS = 256
/** Sends waiting for a turn; a provider that never opens one cannot grow this without bound. */
const MAX_PENDING_INPUTS = 64

export type ProviderTimelineOpenTurn = ProviderTimelineTurnRef & {
  running: AgentJournalTurnLifecycle
}

export type ProviderTimelinePendingInput = { clientMessageId: string; requestedAt: number }

export type ProviderTimelineTurnStatus = 'absent' | 'running' | 'settled'

export class ProviderTimelineTurns {
  private current: ProviderTimelineOpenTurn | null = null
  private previous: ProviderTimelineTurnRef | null = null
  private readonly known = new Map<string, 'running' | 'settled'>()
  private inputs: ProviderTimelinePendingInput[] = []

  constructor(private readonly addresses: ProviderTimelineAddresses) {}

  get open(): ProviderTimelineOpenTurn | null {
    return this.current
  }

  /** The open turn, else the last one to settle: where context facts land. */
  get latest(): ProviderTimelineTurnRef | null {
    return this.current ?? this.previous
  }

  get scope(): AgentJournalTurnScope {
    return this.current
      ? { kind: 'turn', turnItemId: this.current.itemId }
      : AGENT_JOURNAL_THREAD_SCOPE
  }

  status(
    itemId: string,
    journal: StructuredAgentSessionTransitionJournal | null
  ): ProviderTimelineTurnStatus {
    const known = this.known.get(itemId)
    if (known) {
      return known
    }
    const row = readAgentJournalTurn(journal?.itemBody(itemId) ?? undefined)
    return row === null ? 'absent' : row.state === 'running' ? 'running' : 'settled'
  }

  /** The running row of a new turn, written only while the journal holds no row for it. */
  planBegin(
    plan: ProviderTimelinePlan,
    ref: ProviderTimelineTurnRef,
    at: number,
    landed: (wrote: boolean) => void
  ): void {
    const input = this.inputs[0]
    const running: AgentJournalTurnLifecycle = {
      turnId: ref.turnId,
      state: 'running',
      userItemId: input
        ? agentJournalSubmissionKey(input.clientMessageId)
        : this.addresses.turnOpener(ref),
      startedAt: at,
      ...(input ? { requestedAt: input.requestedAt } : {})
    }
    const body = agentJournalTurnBody(running)
    plan.step(
      {
        kind: 'item',
        reservedBytes: estimateStructuredAgentSessionItemBytes(ref.identity, body),
        resolve: (journal) =>
          journal.itemBody(ref.itemId) === null ? { identity: ref.identity, body } : null,
        // The running row's ts is the turn start itself, so clients read no append lag.
        options: { turnScope: AGENT_JOURNAL_THREAD_SCOPE, lifecycle: true, observedAt: at }
      },
      { lifecycle: true, landed }
    )
    plan.commit(() => {
      if (input) {
        this.inputs.shift()
      }
      this.current = { ...ref, running }
      this.remember(ref.itemId, 'running')
    })
  }

  /** A send reached the provider: it names the open turn if nothing opened that one, else the next. */
  planInput(plan: ProviderTimelinePlan, input: ProviderTimelinePendingInput): void {
    const open = this.current
    if (!open) {
      plan.commit(() => {
        this.inputs.push(input)
        this.inputs.splice(0, Math.max(0, this.inputs.length - MAX_PENDING_INPUTS))
      })
      return
    }
    if (open.running.userItemId !== this.addresses.turnOpener(open)) {
      return
    }
    const userItemId = agentJournalSubmissionKey(input.clientMessageId)
    const running = { ...open.running, userItemId, requestedAt: input.requestedAt }
    const target = { identity: open.identity }
    const write = { lifecycle: agentJournalTurnBody(running), onlyWhileRunning: true as const }
    const reservedBytes = agentJournalTurnRowReservedBytes(target, write)
    plan.step(
      {
        kind: 'item',
        reservedBytes,
        // Only the opener fields change; the rest are the row's as the journal holds it.
        resolve: (journal) => {
          const row = readAgentJournalTurn(journal.itemBody(open.itemId) ?? undefined)
          return row?.state === 'running'
            ? resolveAgentJournalTurnRowWrite(
                journal,
                target,
                {
                  lifecycle: agentJournalTurnBody({
                    ...row,
                    userItemId,
                    requestedAt: input.requestedAt
                  }),
                  onlyWhileRunning: true
                },
                reservedBytes
              )
            : null
        },
        options: { turnScope: AGENT_JOURNAL_THREAD_SCOPE, lifecycle: true }
      },
      { lifecycle: true }
    )
    plan.commit(() => {
      if (this.current === open) {
        open.running = running
      }
    })
  }

  /** The turn is over; called when its end is admitted. */
  commitEnd(ref: ProviderTimelineTurnRef): void {
    if (this.current?.itemId === ref.itemId) {
      this.current = null
    }
    this.previous = ref
    this.remember(ref.itemId, 'settled')
  }

  /** A planned open found its row already written (the journal had not finished importing it):
   *  the open turn is whatever the journal holds running, not the one memory assumed. */
  rederive(openedItemId: string, journal: StructuredAgentSessionTransitionJournal | null): void {
    if (this.current?.itemId !== openedItemId) {
      return
    }
    this.remember(openedItemId, 'settled')
    this.current = null
    journal?.visitItems((itemId, _sequence, body) => {
      const running = readAgentJournalTurn(body)
      const identity = running?.state === 'running' ? parseAgentJournalItemKey(itemId) : null
      if (running && identity) {
        this.current = {
          address: {
            namespace: this.addresses.namespace,
            key: { source: 'provider', value: running.turnId }
          },
          identity,
          itemId: agentJournalItemKey(identity),
          turnId: running.turnId,
          running
        }
      }
    })
  }

  reset(): void {
    this.current = null
    this.previous = null
    this.known.clear()
    this.inputs = []
  }

  private remember(itemId: string, status: 'running' | 'settled'): void {
    this.known.delete(itemId)
    this.known.set(itemId, status)
    for (const oldest of this.known.keys()) {
      if (this.known.size <= MAX_KNOWN_TURNS) {
        break
      }
      this.known.delete(oldest)
    }
  }
}
