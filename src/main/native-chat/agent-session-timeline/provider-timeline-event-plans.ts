// How each content event plans its writes: items, text, requests, context facts and frames.
//
// Each planner first decides whether the event is new work — a replay or a duplicate writes
// nothing and is no barrier — then flushes the text owed ahead of it, then adds its own writes.

import {
  AGENT_JOURNAL_THREAD_SCOPE,
  type AgentJournalProducerLinkage,
  type AgentJournalTurnScope,
  type AgentType
} from '../../../shared/agent-session-journal-types'
import { unhandledProviderFrameJournalItem } from '../agent-session-wire/unhandled-provider-frame'
import { estimateStructuredAgentSessionItemBytes } from '../agent-session-wire/structured-agent-session-event-sink-estimate'
import type { StructuredAgentSessionTransitionJournal } from '../agent-session-wire/structured-agent-session-transition'
import {
  agentJournalTurnRowReservedBytes,
  resolveAgentJournalTurnRowWrite
} from './agent-journal-turn-row-revision'
import type { ProviderTimelineAddresses } from './provider-timeline-addresses'
import type { ProviderTimelineEvent, ProviderTimelineJoin } from './provider-timeline-event'
import { providerTimelineItemClass, type ProviderTimelineKey } from './provider-timeline-identity'
import type { ProviderTimelineItems } from './provider-timeline-items'
import type { ProviderTimelinePlan } from './provider-timeline-plan'
import { cancelledJournalPromptBody } from '../agent-session-journal/journal-prompt-body-bounds'
import type { ProviderTimelineTextStreams } from './provider-timeline-text-streams'
import type { ProviderTimelineTurns } from './provider-timeline-turns'

/** Why an event wrote nothing. Each is a grammar rule the adapter broke or a replay it repeated. */
export type ProviderTimelineDropRule =
  | 'session-ended'
  | 'turn-duplicate'
  | 'turn-replayed'
  | 'turn-unknown'
  | 'no-turn'
  | 'item-settled'
  | 'item-replayed'
  | 'request-duplicate'
  | 'request-replayed'
  | 'request-unknown'
  | 'stream-unknown'
  | 'stream-mismatch'

export type ProviderTimelineContext = {
  sessionId: string
  agent: AgentType
  /** The session's own provider thread; a join naming another thread is subagent work. */
  ownThread?: () => string | null
  addresses: ProviderTimelineAddresses
  turns: ProviderTimelineTurns
  items: ProviderTimelineItems
  streams: ProviderTimelineTextStreams
  journal: () => StructuredAgentSessionTransitionJournal | null
  /** A settlement id no other settlement of this journal shares. */
  settlementId: (what: string) => string
}

type Placement = {
  thread: string | null
  turn: ProviderTimelineKey | null
  scope: AgentJournalTurnScope
}

/** The turn a row joins: the one the provider names (subagent work joins the open turn), else the open one. */
export function providerTimelinePlacement(
  context: ProviderTimelineContext,
  join: ProviderTimelineJoin | undefined
): Placement {
  const thread = join?.thread ?? null
  const open = context.turns.open
  if (join?.turn === undefined) {
    return { thread, turn: open?.address.key ?? null, scope: context.turns.scope }
  }
  const key: ProviderTimelineKey = { source: 'provider', value: join.turn }
  const own = context.ownThread?.() ?? null
  const subagent = thread !== null && own !== null && thread !== own
  return {
    thread,
    turn: key,
    scope: subagent
      ? context.turns.scope
      : { kind: 'turn', turnItemId: context.addresses.turn(key).itemId }
  }
}

/** Text owed ahead of this event; a row-writing event also ends its producer's anonymous message. */
export function planProviderTimelineBarrier(
  context: ProviderTimelineContext,
  plan: ProviderTimelinePlan,
  options: { boundary: boolean; producer?: AgentJournalProducerLinkage; except?: string }
): void {
  context.streams.planFlush(plan, options.except)
  if (options.boundary) {
    const agentId = options.producer?.agentId
    context.streams.planRelease(
      plan,
      (stream) => stream.anonymous && stream.producer?.agentId === agentId
    )
  }
}

type ItemEvent = Extract<
  ProviderTimelineEvent,
  { type: 'item.open' | 'item.update' | 'item.close' }
>

export function planProviderTimelineItem(
  context: ProviderTimelineContext,
  plan: ProviderTimelinePlan,
  event: ItemEvent
): ProviderTimelineDropRule | null {
  const change =
    event.type === 'item.open' ? 'open' : event.type === 'item.update' ? 'update' : 'close'
  const row = context.addresses.row(
    {
      family: 'item',
      key: { source: 'provider', value: event.item },
      itemClass: providerTimelineItemClass(event.body)
    },
    providerTimelinePlacement(context, event.join)
  )
  const dropped = context.items.judge(change, row, event.body, context.journal())
  if (dropped) {
    return dropped
  }
  // A full snapshot of a streamed message replaces what streamed, so that text is not flushed.
  const stream = change === 'close' ? context.streams.get({ id: event.item }) : undefined
  planProviderTimelineBarrier(context, plan, {
    boundary: true,
    ...(event.producer ? { producer: event.producer } : {}),
    ...(stream ? { except: row.itemId } : {})
  })
  context.items.plan(plan, change, row, event.body, {
    ...(event.producer ? { producer: event.producer } : {}),
    ...(event.type === 'item.open' && event.outlivesTurn ? { outlivesTurn: true } : {})
  })
  if (stream) {
    context.streams.planRelease(plan, (each) => each === stream)
  }
  plan.commit(() => context.addresses.remember(row))
  return null
}

export function planProviderTimelineText(
  context: ProviderTimelineContext,
  plan: ProviderTimelinePlan,
  event: Extract<ProviderTimelineEvent, { type: 'text.delta' }>
): ProviderTimelineDropRule | null {
  const start = { item: event.item, channel: event.channel, producer: event.producer }
  const named = 'id' in event.item ? event.item.id : null
  let stream = context.streams.get(event.item)
  if (stream && !context.streams.continues(stream, start)) {
    if (named !== null) {
      return 'stream-mismatch'
    }
    // The anonymous stream's next message: what the last one owes lands first.
    context.streams.planFlush(plan)
    const ended = stream
    context.streams.planRelease(plan, (each) => each === ended)
    stream = undefined
  }
  if (!stream) {
    const itemClass = event.channel === 'assistant' ? 'message' : 'reasoning'
    const key =
      named === null ? context.addresses.mint('s') : ({ source: 'provider', value: named } as const)
    const row = context.addresses.row(
      { family: 'item', key, itemClass },
      providerTimelinePlacement(context, event.join)
    )
    if (named !== null && context.items.isClosed(row.itemId)) {
      return 'item-settled'
    }
    stream = context.streams.planStart(plan, row, start) ?? undefined
    if (!stream) {
      return null
    }
    if (named !== null) {
      context.items.planStreamed(plan, row)
      plan.commit(() => context.addresses.remember(row))
    }
  }
  context.streams.planAppend(plan, stream, event.text)
  return null
}

export function planProviderTimelineTextClose(
  context: ProviderTimelineContext,
  plan: ProviderTimelinePlan,
  event: Extract<ProviderTimelineEvent, { type: 'text.close' }>
): ProviderTimelineDropRule | null {
  const stream = context.streams.get(event.item)
  if (!stream) {
    return 'stream-unknown'
  }
  context.streams.planFlush(plan, stream.row.itemId)
  context.streams.planClose(plan, stream, event.text)
  return null
}

export function planProviderTimelineRequest(
  context: ProviderTimelineContext,
  plan: ProviderTimelinePlan,
  event: Extract<ProviderTimelineEvent, { type: 'request.open' }>
): ProviderTimelineDropRule | null {
  const placement = providerTimelinePlacement(context, event.join)
  const rowFor = (incarnation: number) =>
    context.addresses.row(
      {
        family: 'request',
        key: { source: 'provider', value: event.request },
        itemClass: event.body.kind,
        incarnation
      },
      placement
    )
  const judged = context.items.judgeRequest(event.request, rowFor, context.journal())
  if ('dropped' in judged) {
    return judged.dropped
  }
  if ('adopt' in judged) {
    context.items.planRequest(plan, judged.adopt, null, event.request)
    return 'request-duplicate'
  }
  planProviderTimelineBarrier(context, plan, {
    boundary: true,
    ...(event.producer ? { producer: event.producer } : {})
  })
  context.items.planRequest(plan, judged.open, event.body, event.request, event.producer)
  plan.commit(() => context.addresses.remember(judged.open))
  return null
}

export function planProviderTimelineWithdrawal(
  context: ProviderTimelineContext,
  plan: ProviderTimelinePlan,
  event: Extract<ProviderTimelineEvent, { type: 'request.withdrawn' }>
): ProviderTimelineDropRule | null {
  const rowFor = (incarnation: number) =>
    context.addresses.known('request', event.request, { incarnation }) ??
    context.addresses.row(
      {
        family: 'request',
        key: { source: 'provider', value: event.request },
        itemClass: 'approval',
        incarnation
      },
      providerTimelinePlacement(context, undefined)
    )
  const row = context.items.pendingRequest(event.request, rowFor, context.journal())
  if (!row) {
    return 'request-unknown'
  }
  planProviderTimelineBarrier(context, plan, { boundary: true })
  plan.step({
    kind: 'settlement',
    // Names the incarnation, so a reopened request's withdrawal is never taken for an earlier one.
    settlementId: context.settlementId(`request-withdrawn:${row.itemId}`),
    reservedBytes: 4096,
    // Only while the journal holds it pending: a client's answer that landed first stands.
    resolve: (journal) => {
      const held = journal.itemBody(row.itemId)
      const pending =
        (held?.kind === 'approval' || held?.kind === 'question') &&
        held.resolution.state === 'pending'
      const body = pending ? cancelledJournalPromptBody(held) : null
      return body ? [{ kind: 'item', identity: row.identity, body, turnScope: row.scope }] : []
    }
  })
  plan.commit(() => context.items.commitWithdrawn(event.request, row))
  return null
}

export function planProviderTimelineContextUsage(
  context: ProviderTimelineContext,
  plan: ProviderTimelinePlan,
  event: Extract<ProviderTimelineEvent, { type: 'context.usage' }>
): void {
  const named = event.join?.turn
  const turn =
    named === undefined
      ? context.turns.latest
      : context.addresses.turn({ source: 'provider', value: named })
  const target = turn ? { identity: turn.identity } : ({ newest: true } as const)
  const write = { contextUsage: event.usage }
  const reservedBytes = agentJournalTurnRowReservedBytes(target, write)
  planProviderTimelineBarrier(context, plan, { boundary: true })
  plan.step({
    kind: 'item',
    reservedBytes,
    resolve: (journal) => resolveAgentJournalTurnRowWrite(journal, target, write, reservedBytes),
    options: { turnScope: AGENT_JOURNAL_THREAD_SCOPE }
  })
}

export function planProviderTimelineFrame(
  context: ProviderTimelineContext,
  plan: ProviderTimelinePlan,
  event: Extract<ProviderTimelineEvent, { type: 'provider.frame' }>
): void {
  const frame = unhandledProviderFrameJournalItem(context.agent, event.frameKind, event.payload)
  if (!frame) {
    return
  }
  const row = context.addresses.row(
    { family: 'frame', key: context.addresses.mint('f'), itemClass: 'frame' },
    providerTimelinePlacement(context, event.join)
  )
  planProviderTimelineBarrier(context, plan, { boundary: true })
  plan.step({
    kind: 'item',
    reservedBytes: estimateStructuredAgentSessionItemBytes(row.identity, frame.body),
    resolve: () => ({ identity: row.identity, body: frame.body }),
    options: { turnScope: row.scope }
  })
}
