// What a turn's end or the session's end settles, read from the journal when the settlement runs.
//
// The same mechanism the dead-generation settlement uses after a restart: every row still waiting
// on the row that settles it — a running tool call, a pending prompt — is settled from its body as
// the journal holds it then. Nothing about open work is trusted from memory, so a prompt a client
// answered a moment earlier stays answered, a row a previous assembler opened is settled too, and
// a second settlement finds nothing left to do.

import { parseAgentJournalItemKey } from '../../../shared/agent-session-journal-item-key'
import {
  AGENT_JOURNAL_THREAD_SCOPE,
  type AgentJournalItemBody,
  type AgentJournalItemIdentity,
  type AgentJournalTurnLifecycle
} from '../../../shared/agent-session-journal-types'
import {
  agentJournalTurnBody,
  readAgentJournalTurn
} from '../../../shared/agent-session-turn-record'
import { cancelledJournalPromptBody } from '../agent-session-journal/journal-prompt-body-bounds'
import type { JournalLifecycleMutationInput } from '../agent-session-journal/journal-row-builders'
import type { StructuredAgentSessionTransitionJournal } from '../agent-session-wire/structured-agent-session-transition'
import {
  agentJournalTurnRowReservedBytes,
  resolveAgentJournalTurnRowWrite
} from './agent-journal-turn-row-revision'

/** How a turn ended: the provider's report, or what the host could prove when the child went. */
export type ProviderTimelineTurnEnd =
  | {
      state: 'completed' | 'interrupted'
      completedAt: number
      outcome?: AgentJournalTurnLifecycle['outcome']
      durationMs?: number
    }
  | { state: 'unverifiable' }

/** What a row still open when its turn or the session ends becomes; null when it already stands. */
export function settledProviderTimelineBody(
  body: AgentJournalItemBody
): AgentJournalItemBody | null {
  if (body.kind === 'tool-call') {
    return body.state === 'running' ? { ...body, state: 'failed' } : null
  }
  if (body.kind === 'approval' || body.kind === 'question') {
    return body.resolution.state === 'pending' ? cancelledJournalPromptBody(body) : null
  }
  return null
}

/** The end owns the turn's terminal fields; `unverifiable` carries no end and no verdict. Fields
 *  keep the order every lane writes them in, so the row is the same bytes whoever ends it. */
export function endedProviderTimelineTurn(
  running: AgentJournalTurnLifecycle,
  end: ProviderTimelineTurnEnd
): AgentJournalTurnLifecycle {
  const { turnId, userItemId, startedAt, requestedAt } = running
  const opened = {
    ...(userItemId !== undefined ? { userItemId } : {}),
    ...(startedAt !== undefined ? { startedAt } : {}),
    ...(requestedAt !== undefined ? { requestedAt } : {})
  }
  if (end.state === 'unverifiable') {
    return { turnId, state: 'unverifiable', ...opened }
  }
  return {
    turnId,
    state: end.state,
    ...(end.outcome !== undefined ? { outcome: end.outcome } : {}),
    ...opened,
    completedAt: end.completedAt,
    ...(end.durationMs !== undefined ? { durationMs: end.durationMs } : {})
  }
}

/** Which open rows a settlement covers: one turn's, or every row when the session ends. */
export type ProviderTimelineSettlementScope =
  | { turnItemId: string; outlivesTurn: ReadonlySet<string> }
  | 'session'

/** The settled rows of `scope`, then each listed turn's end while the journal holds it running. */
export function providerTimelineSettlement(
  journal: StructuredAgentSessionTransitionJournal,
  scope: ProviderTimelineSettlementScope,
  turns: readonly { identity: AgentJournalItemIdentity; itemId: string }[],
  end: ProviderTimelineTurnEnd
): JournalLifecycleMutationInput[] {
  const mutations: JournalLifecycleMutationInput[] = []
  journal.visitItemsWithLinkage((itemId, _sequence, body, attribution) => {
    const turnScope = attribution.turnScope ?? AGENT_JOURNAL_THREAD_SCOPE
    const covered =
      scope === 'session' ||
      (turnScope.kind === 'turn' &&
        turnScope.turnItemId === scope.turnItemId &&
        !scope.outlivesTurn.has(itemId))
    const settled = covered ? settledProviderTimelineBody(body) : null
    const identity = settled ? parseAgentJournalItemKey(itemId) : null
    if (settled && identity) {
      // No linkage: a revision keeps the row's own producer.
      mutations.push({ kind: 'item', identity, body: settled, turnScope })
    }
  })
  for (const turn of turns) {
    const ended = endedTurnRow(journal, turn, end)
    if (ended) {
      mutations.push({ kind: 'item', ...ended, turnScope: AGENT_JOURNAL_THREAD_SCOPE })
    }
  }
  return mutations
}

/** Every turn row the journal holds running, for the session's end. */
export function runningProviderTimelineTurns(
  journal: StructuredAgentSessionTransitionJournal
): { identity: AgentJournalItemIdentity; itemId: string }[] {
  const running: { identity: AgentJournalItemIdentity; itemId: string }[] = []
  journal.visitItems((itemId, _sequence, body) => {
    const identity =
      readAgentJournalTurn(body)?.state === 'running' ? parseAgentJournalItemKey(itemId) : null
    if (identity) {
      running.push({ identity, itemId })
    }
  })
  return running
}

function endedTurnRow(
  journal: StructuredAgentSessionTransitionJournal,
  turn: { identity: AgentJournalItemIdentity; itemId: string },
  end: ProviderTimelineTurnEnd
): { identity: AgentJournalItemIdentity; body: AgentJournalItemBody } | null {
  const running = readAgentJournalTurn(journal.itemBody(turn.itemId) ?? undefined)
  if (running?.state !== 'running') {
    return null
  }
  const target = { identity: turn.identity }
  const write = {
    lifecycle: agentJournalTurnBody(endedProviderTimelineTurn(running, end)),
    onlyWhileRunning: true as const
  }
  return resolveAgentJournalTurnRowWrite(
    journal,
    target,
    write,
    agentJournalTurnRowReservedBytes(target, write)
  )
}
