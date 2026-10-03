// Streamed text, as message rows.
//
// A stream the provider names is the item with that id, so its deltas and any full snapshot of it
// are one row. An anonymous stream becomes a new message each time it starts, and its identity is
// its name, channel and producer: a change to any starts the next message. Deltas accumulate in
// the shared coalescer, and a row is a snapshot of the text so far. Text owed to the journal is
// written inside the next event's transition — every other event is an ordering barrier — or by
// the coalescer's window; either way it is marked written only once the sink takes it.

import type { AgentJournalProducerLinkage } from '../../../shared/agent-session-journal-types'
import {
  boundInlineText,
  DEFAULT_JOURNAL_PAYLOAD_LIMITS
} from '../agent-session-journal/journal-payload-bounds'
import {
  createAgentSessionDeltaCoalescer,
  type AgentSessionDeltaCoalescerDeps
} from '../agent-session-wire/agent-session-delta-coalescer'
import { estimateStructuredAgentSessionItemBytes } from '../agent-session-wire/structured-agent-session-event-sink-estimate'
import type { ProviderTimelineRow } from './provider-timeline-addresses'
import {
  PROVIDER_TIMELINE_OVER_BUDGET,
  providerTimelineEntryBytes,
  type ProviderTimelineBudget
} from './provider-timeline-budget'
import type {
  ProviderTimelineTextChannel,
  ProviderTimelineTextItem
} from './provider-timeline-event'
import { ProviderTimelinePlan, type ProviderTimelineSink } from './provider-timeline-plan'

/** One open text stream: the row it writes and what continues it. */
export type ProviderTimelineStream = {
  key: string
  row: ProviderTimelineRow
  channel: ProviderTimelineTextChannel
  producer: AgentJournalProducerLinkage | undefined
  anonymous: boolean
  /** Whether any text reached the journal; a whitespace-only stream completes nothing. */
  written: boolean
}

export type ProviderTimelineStreamStart = {
  item: ProviderTimelineTextItem
  channel: ProviderTimelineTextChannel
  producer: AgentJournalProducerLinkage | undefined
}

export function providerTimelineStreamKey(item: ProviderTimelineTextItem): string {
  return 'id' in item ? `id:${item.id}` : `stream:${item.stream}`
}

export class ProviderTimelineTextStreams {
  private readonly streams = new Map<string, ProviderTimelineStream>()
  private readonly byItemId = new Map<string, ProviderTimelineStream>()
  private readonly coalescer

  constructor(
    private readonly deps: {
      sink: ProviderTimelineSink
      budget: ProviderTimelineBudget
      coalesceMs?: number
      schedule?: AgentSessionDeltaCoalescerDeps['schedule']
    }
  ) {
    this.coalescer = createAgentSessionDeltaCoalescer({
      ...(deps.coalesceMs === undefined ? {} : { windowMs: deps.coalesceMs }),
      ...(deps.schedule ? { schedule: deps.schedule } : {}),
      // Every stream here is one this assembler holds open; the shared budget counts them.
      isProtected: () => true,
      emit: (itemId, text) => this.flushOnWindow(itemId, text)
    })
  }

  get(item: ProviderTimelineTextItem): ProviderTimelineStream | undefined {
    return this.streams.get(providerTimelineStreamKey(item))
  }

  /** Whether `start` continues `stream` or begins another message. */
  continues(stream: ProviderTimelineStream, start: ProviderTimelineStreamStart): boolean {
    return stream.channel === start.channel && stream.producer?.agentId === start.producer?.agentId
  }

  /** A new stream into `row`; false when the budget cannot hold it. */
  planStart(
    plan: ProviderTimelinePlan,
    row: ProviderTimelineRow,
    start: ProviderTimelineStreamStart
  ): ProviderTimelineStream | null {
    const bytes = providerTimelineEntryBytes(row.itemId)
    if (!this.deps.budget.admits(row.itemId, bytes)) {
      plan.refuse(PROVIDER_TIMELINE_OVER_BUDGET)
      return null
    }
    const stream: ProviderTimelineStream = {
      key: providerTimelineStreamKey(start.item),
      row,
      channel: start.channel,
      producer: start.producer,
      anonymous: !('id' in start.item),
      written: false
    }
    plan.commit(() => {
      this.deps.budget.hold(row.itemId, bytes)
      this.streams.set(stream.key, stream)
      this.byItemId.set(row.itemId, stream)
    })
    return stream
  }

  /** Text arrives after the event is admitted; it changes no row by itself. */
  planAppend(plan: ProviderTimelinePlan, stream: ProviderTimelineStream, text: string): void {
    plan.commit(() => {
      this.coalescer.append(stream.row.itemId, text)
    })
  }

  /** Every stream's unwritten text, ahead of whatever the event writes. */
  planFlush(plan: ProviderTimelinePlan, except?: string): void {
    for (const { key, snapshot } of this.coalescer.dirty()) {
      const stream = this.byItemId.get(key)
      if (stream && key !== except) {
        this.planText(plan, stream, snapshot.text)
      }
    }
  }

  /** Ends the streams `which` selects; their text was flushed by the same event. */
  planRelease(
    plan: ProviderTimelinePlan,
    which: (stream: ProviderTimelineStream) => boolean
  ): void {
    const released = [...this.streams.values()].filter(which)
    plan.commit(() => released.forEach((stream) => this.forget(stream)))
  }

  /** Ends one stream with the provider's final text, else what streamed. */
  planClose(plan: ProviderTimelinePlan, stream: ProviderTimelineStream, finalText?: string): void {
    if (finalText === undefined) {
      const owed = this.coalescer.snapshot(stream.row.itemId)
      if (owed && this.coalescer.dirty().some(({ key }) => key === stream.row.itemId)) {
        this.planText(plan, stream, owed.text)
      }
    } else {
      this.planText(
        plan,
        stream,
        boundInlineText(finalText, DEFAULT_JOURNAL_PAYLOAD_LIMITS).text,
        true
      )
    }
    plan.commit(() => this.forget(stream))
  }

  /** The window's flush: false keeps the text for a retry, but only while the sink is merely full. */
  private flushOnWindow(itemId: string, text: string): boolean {
    const stream = this.byItemId.get(itemId)
    if (!stream) {
      return true
    }
    const plan = new ProviderTimelinePlan()
    this.planText(plan, stream, text)
    const admission = plan.submit(this.deps.sink)
    // A failed or closed sink can never take it; the session's end owns what it leaves.
    return admission.accepted || admission.reason !== 'backpressure'
  }

  flush(): void {
    this.coalescer.flushAll()
  }

  reset(): void {
    for (const stream of this.streams.values()) {
      this.forget(stream)
    }
  }

  dispose(): void {
    this.reset()
    this.coalescer.dispose()
  }

  private planText(
    plan: ProviderTimelinePlan,
    stream: ProviderTimelineStream,
    text: string,
    providerFinal = false
  ): void {
    const empty = providerFinal ? text.length === 0 : text.trim().length === 0
    if (!stream.written && empty) {
      plan.commit(() => this.coalescer.markFlushed(stream.row.itemId))
      return
    }
    const { identity, scope } = stream.row
    const body = {
      kind: 'message' as const,
      role: stream.channel === 'assistant' ? ('assistant' as const) : ('reasoning' as const),
      blocks: [{ type: 'text' as const, text }]
    }
    plan.step({
      kind: 'item',
      reservedBytes: estimateStructuredAgentSessionItemBytes(identity, body),
      resolve: () => ({ identity, body }),
      options: { ...stream.producer, turnScope: scope }
    })
    plan.commit(() => {
      stream.written = true
      this.coalescer.markFlushed(stream.row.itemId)
    })
  }

  private forget(stream: ProviderTimelineStream): void {
    this.coalescer.forget(stream.row.itemId)
    this.deps.budget.release(stream.row.itemId)
    if (this.streams.get(stream.key) === stream) {
      this.streams.delete(stream.key)
    }
    this.byItemId.delete(stream.row.itemId)
  }
}
