// Turns a provider adapter's grammar events (`provider-timeline-event.ts`) into the journal rows
// every structured lane writes.
//
// Division of lifecycle work:
// - The adapter translates its dialect, decides when a turn opens, and re-applies an event the sink
//   refused (the lane runner holds it and pauses reading under backpressure).
// - The assembler resolves provider joins to rows (identity, turn scope, ordinals, namespaces),
//   bounds what it holds open, and plans each event as ONE sink transition, changing its memory
//   only when the sink admits it. A refused event leaves it unchanged.
// - The journal decides, when each write runs, what a replay or a race changes: a turn row exists
//   or is still running, a prompt is still pending, a tool already settled. Turn and session ends
//   settle every row the journal still holds open — the dead-generation settlement's mechanism —
//   and a person's Stop, a dead generation after a restart, and the answer compare-and-set stay
//   the journal's as before.

import type { AgentType } from '../../../shared/agent-session-journal-types'
import type { AgentSessionDeltaCoalescerDeps } from '../agent-session-wire/agent-session-delta-coalescer'
import type { StructuredAgentSessionSinkAdmission } from '../agent-session-wire/structured-agent-session-event-sink'
import {
  ProviderTimelineAddresses,
  type ProviderTimelineTurnRef
} from './provider-timeline-addresses'
import { ProviderTimelineBudget } from './provider-timeline-budget'
import type { ProviderTimelineEvent } from './provider-timeline-event'
import {
  planProviderTimelineBarrier,
  planProviderTimelineContextUsage,
  planProviderTimelineFrame,
  planProviderTimelineItem,
  planProviderTimelineRequest,
  planProviderTimelineText,
  planProviderTimelineTextClose,
  planProviderTimelineWithdrawal,
  type ProviderTimelineContext,
  type ProviderTimelineDropRule
} from './provider-timeline-event-plans'
import {
  createLegacyProviderTimelineIdentityScheme,
  type ProviderTimelineIdentityScheme,
  type ProviderTimelineKey
} from './provider-timeline-identity'
import { ProviderTimelineItems } from './provider-timeline-items'
import { ProviderTimelinePlan, type ProviderTimelineSink } from './provider-timeline-plan'
import {
  providerTimelineSettlement,
  runningProviderTimelineTurns,
  type ProviderTimelineTurnEnd
} from './provider-timeline-settlement'
import { ProviderTimelineTextStreams } from './provider-timeline-text-streams'
import { ProviderTimelineTurns } from './provider-timeline-turns'

export type { ProviderTimelineDropRule } from './provider-timeline-event-plans'

export type ProviderTimelineApplyResult = {
  /** The sink's answer for the event's writes; refused means nothing changed. */
  admission: StructuredAgentSessionSinkAdmission
  dropped?: ProviderTimelineDropRule
}

export type ProviderTimelineAssembler = {
  apply(event: ProviderTimelineEvent): ProviderTimelineApplyResult
  /** The turn id of the open turn, as its row and a client's Stop name it. */
  readonly openTurnId: string | null
  /** Writes the text the coalescing window holds. */
  flush(): void
  dispose(): void
}

export type ProviderTimelineAssemblerDeps = {
  sink: ProviderTimelineSink
  sessionId: string
  agent: AgentType
  /** The acquisition: minted keys are unique per generation. */
  generation: string
  /** The provider session whose ids the adapter forwards; the same one again after a restart that
   *  re-attaches it, so its replays are recognised. */
  namespace: string
  /** The session's own provider thread, for providers that run subagents on threads of their own. */
  ownThread?: () => string | null
  /** Defaults to the shared `legacy` identity arm. */
  scheme?: ProviderTimelineIdentityScheme
  coalesceMs?: number
  schedule?: AgentSessionDeltaCoalescerDeps['schedule']
}

/** Room for a settled turn row and its context facts. */
const SETTLEMENT_RESERVED_BYTES = 64 * 1024

export function createProviderTimelineAssembler(
  deps: ProviderTimelineAssemblerDeps
): ProviderTimelineAssembler {
  const scheme =
    deps.scheme ??
    createLegacyProviderTimelineIdentityScheme({ agent: deps.agent, sessionId: deps.sessionId })
  const addresses = new ProviderTimelineAddresses({
    scheme,
    generation: deps.generation,
    namespace: deps.namespace
  })
  const budget = new ProviderTimelineBudget()
  const turns = new ProviderTimelineTurns(addresses)
  const items = new ProviderTimelineItems(budget)
  const streams = new ProviderTimelineTextStreams({
    sink: deps.sink,
    budget,
    ...(deps.coalesceMs === undefined ? {} : { coalesceMs: deps.coalesceMs }),
    ...(deps.schedule ? { schedule: deps.schedule } : {})
  })
  let ended = false
  let settlements = 0
  const context: ProviderTimelineContext = {
    sessionId: deps.sessionId,
    agent: deps.agent,
    ...(deps.ownThread ? { ownThread: deps.ownThread } : {}),
    addresses,
    turns,
    items,
    streams,
    journal: () => deps.sink.journalItems(),
    settlementId: (what) => {
      settlements += 1
      return `provider-timeline:${deps.sessionId}:${deps.generation}:${settlements}:${what}`
    }
  }

  /** The turn's open rows and its row settle in one journal-derived batch. */
  const planTurnEnd = (
    plan: ProviderTimelinePlan,
    turn: ProviderTimelineTurnRef,
    end: ProviderTimelineTurnEnd,
    onlyWhileAbsent?: string
  ): void => {
    const scope = { turnItemId: turn.itemId, outlivesTurn: items.outlivingTurn() }
    plan.step({
      kind: 'settlement',
      settlementId: context.settlementId(`turn-end:${turn.itemId}`),
      reservedBytes: SETTLEMENT_RESERVED_BYTES,
      resolve: (journal) =>
        // A supersede is void when the turn replacing it was already written (a replay).
        onlyWhileAbsent !== undefined && journal.itemBody(onlyWhileAbsent) !== null
          ? []
          : providerTimelineSettlement(journal, scope, [turn], end)
    })
    streams.planRelease(
      plan,
      (stream) => stream.row.scope.kind === 'turn' && stream.row.scope.turnItemId === turn.itemId
    )
    plan.commit(() => {
      turns.commitEnd(turn)
      items.commitTurnEnd(turn.itemId)
      deps.sink.setActivity?.(null)
    })
  }

  const planSessionEnd = (plan: ProviderTimelinePlan, end: ProviderTimelineTurnEnd): void => {
    streams.planFlush(plan)
    plan.step({
      kind: 'settlement',
      settlementId: context.settlementId('session-end'),
      reservedBytes: SETTLEMENT_RESERVED_BYTES,
      resolve: (journal) =>
        providerTimelineSettlement(journal, 'session', runningProviderTimelineTurns(journal), end)
    })
    plan.commit(() => {
      ended = true
      streams.reset()
      items.reset()
      turns.reset()
      deps.sink.setActivity?.(null)
    })
  }

  const planTurnOpen = (
    plan: ProviderTimelinePlan,
    event: Extract<ProviderTimelineEvent, { type: 'turn.open' }>
  ): ProviderTimelineDropRule | null => {
    const open = turns.open
    if (event.turn === undefined && open) {
      return 'turn-duplicate'
    }
    const key: ProviderTimelineKey =
      event.turn === undefined ? addresses.mint('t') : { source: 'provider', value: event.turn }
    const turn = addresses.turn(key)
    if (open?.itemId === turn.itemId) {
      return 'turn-duplicate'
    }
    if (turns.status(turn.itemId, context.journal()) !== 'absent') {
      return 'turn-replayed'
    }
    planProviderTimelineBarrier(context, plan, { boundary: true })
    if (open) {
      // A newer turn ended this one, whoever asked for it.
      planTurnEnd(
        plan,
        open,
        { state: 'interrupted', completedAt: event.at, outcome: 'superseded' },
        turn.itemId
      )
    }
    turns.planBegin(plan, turn, event.at, (wrote) => {
      if (!wrote) {
        turns.rederive(turn.itemId, context.journal())
      }
    })
    plan.commit(() => deps.sink.setActivity?.(null))
    return null
  }

  const planTurnClose = (
    plan: ProviderTimelinePlan,
    event: Extract<ProviderTimelineEvent, { type: 'turn.end' }>
  ): ProviderTimelineDropRule | null => {
    const turn =
      event.turn === undefined
        ? turns.open
        : addresses.turn({ source: 'provider', value: event.turn })
    if (!turn) {
      return 'no-turn'
    }
    if (
      turns.open?.itemId !== turn.itemId &&
      turns.status(turn.itemId, context.journal()) !== 'running'
    ) {
      return 'turn-unknown'
    }
    planProviderTimelineBarrier(context, plan, { boundary: true })
    planTurnEnd(plan, turn, {
      state: event.state,
      completedAt: event.at,
      ...(event.outcome !== undefined ? { outcome: event.outcome } : {}),
      ...(event.durationMs !== undefined ? { durationMs: event.durationMs } : {})
    })
    return null
  }

  const planEvent = (
    plan: ProviderTimelinePlan,
    event: Exclude<ProviderTimelineEvent, { type: 'session.reset' }>
  ): ProviderTimelineDropRule | null => {
    switch (event.type) {
      case 'input.accepted': {
        const turn = event.join?.turn
        const key: ProviderTimelineKey | null =
          turn === undefined
            ? (turns.open?.address.key ?? null)
            : { source: 'provider', value: turn }
        if (key && event.join?.item !== undefined) {
          addresses.reserveMessage(event.join.thread ?? null, key, event.join.item)
        }
        // A late echo of a turn already over names nothing.
        if (
          turn === undefined ||
          addresses.turn({ source: 'provider', value: turn }).itemId === turns.open?.itemId
        ) {
          planProviderTimelineBarrier(context, plan, { boundary: false })
          turns.planInput(plan, event)
        }
        return null
      }
      case 'turn.open':
        return planTurnOpen(plan, event)
      case 'turn.end':
        return planTurnClose(plan, event)
      case 'item.open':
      case 'item.update':
      case 'item.close':
        return planProviderTimelineItem(context, plan, event)
      case 'text.delta':
        return planProviderTimelineText(context, plan, event)
      case 'text.close':
        return planProviderTimelineTextClose(context, plan, event)
      case 'request.open':
        return planProviderTimelineRequest(context, plan, event)
      case 'request.withdrawn':
        return planProviderTimelineWithdrawal(context, plan, event)
      case 'context.usage':
        planProviderTimelineContextUsage(context, plan, event)
        return null
      case 'activity': {
        const open = turns.open
        if (!open) {
          return 'no-turn'
        }
        deps.sink.setActivity?.(
          event.text === null ? null : { turnId: open.turnId, text: event.text }
        )
        return null
      }
      case 'provider.frame':
        planProviderTimelineFrame(context, plan, event)
        return null
      case 'session.ended':
        planSessionEnd(plan, event.verdict)
        return null
    }
  }

  const apply = (event: ProviderTimelineEvent): ProviderTimelineApplyResult => {
    const next = new ProviderTimelinePlan()
    let dropped: ProviderTimelineDropRule | null = null
    if (event.type === 'session.reset') {
      // Nothing ended the old session's open work, so it is lost, not interrupted.
      if (!ended && (turns.open || items.hasOpenWork())) {
        planSessionEnd(next, { state: 'unverifiable' })
      }
      next.commit(() => {
        ended = false
        streams.reset()
        items.reset()
        turns.reset()
        addresses.reset(event.namespace)
      })
    } else if (ended) {
      dropped = 'session-ended'
    } else {
      dropped = planEvent(next, event)
    }
    const admission = next.submit(deps.sink)
    return dropped ? { admission, dropped } : { admission }
  }

  return {
    apply,
    get openTurnId() {
      return turns.open?.turnId ?? null
    },
    flush: () => streams.flush(),
    dispose: () => {
      streams.dispose()
      items.reset()
      turns.reset()
    }
  }
}
