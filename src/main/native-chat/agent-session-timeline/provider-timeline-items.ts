// Items and requests: what this assembler opened, and how a new event for one is judged.
//
// Memory knows what this assembler wrote in its lifetime; the journal knows every row. A row the
// journal holds that this assembler never wrote is a replay, and a replay never changes it: a
// settled tool is not reopened, an answered prompt is not reset. Within its lifetime, a key whose
// item settled may open again (the same row) and a key whose request settled is a new request (a
// new row, so the answered one keeps its answer). Every write re-checks the journal when it runs.

import type {
  AgentJournalItemBody,
  AgentJournalProducerLinkage
} from '../../../shared/agent-session-journal-types'
import { requiresTerminalSettlement } from '../agent-session-journal/journal-terminal-settlement'
import { estimateStructuredAgentSessionItemBytes } from '../agent-session-wire/structured-agent-session-event-sink-estimate'
import type { StructuredAgentSessionTransitionJournal } from '../agent-session-wire/structured-agent-session-transition'
import type { ProviderTimelineRow } from './provider-timeline-addresses'
import {
  PROVIDER_TIMELINE_OVER_BUDGET,
  providerTimelineEntryBytes,
  type ProviderTimelineBudget
} from './provider-timeline-budget'
import type { ProviderTimelinePlan } from './provider-timeline-plan'
import { settledProviderTimelineBody } from './provider-timeline-settlement'

/** Settled keys remembered so a repeat is recognised without asking the journal. */
const MAX_SETTLED_KEYS = 512

export type ProviderTimelineItemChange = 'open' | 'update' | 'close'

type Tracked = {
  row: ProviderTimelineRow
  /** A close (or its turn's end) settled it. */
  closed: boolean
  /** Its row still waits on a settlement (a running tool, a backgrounded task). */
  obligation: boolean
  outlivesTurn: boolean
}
type Request = { row: ProviderTimelineRow; incarnation: number; pending: boolean }

function settledTool(body: AgentJournalItemBody | null | undefined): boolean {
  return body?.kind === 'tool-call' && body.state !== 'running'
}

function runningTool(body: AgentJournalItemBody): boolean {
  return body.kind === 'tool-call' && body.state === 'running'
}

export class ProviderTimelineItems {
  private readonly items = new Map<string, Tracked>()
  private readonly requests = new Map<string, Request>()

  constructor(private readonly budget: ProviderTimelineBudget) {}

  isClosed(itemId: string): boolean {
    return this.items.get(itemId)?.closed === true
  }

  /** A provider-named message began streaming: this assembler wrote that row. */
  planStreamed(plan: ProviderTimelinePlan, row: ProviderTimelineRow): void {
    plan.commit(() => {
      if (!this.items.has(row.itemId)) {
        this.track(row.itemId, { row, closed: false, obligation: false, outlivesTurn: false })
      }
    })
  }

  /** Why the change writes nothing, or null when it is new work. */
  judge(
    change: ProviderTimelineItemChange,
    row: ProviderTimelineRow,
    body: AgentJournalItemBody,
    journal: StructuredAgentSessionTransitionJournal | null
  ): 'item-replayed' | 'item-settled' | null {
    const known = this.items.get(row.itemId)
    const held = journal?.itemBody(row.itemId) ?? null
    if (change === 'open') {
      return !known && held ? 'item-replayed' : null
    }
    if (change === 'update') {
      return runningTool(body) && (known?.closed || settledTool(held)) ? 'item-settled' : null
    }
    return known?.closed || (!known && settledTool(held)) ? 'item-settled' : null
  }

  /** The item's write, guarded again by the journal when it runs. */
  plan(
    plan: ProviderTimelinePlan,
    change: ProviderTimelineItemChange,
    row: ProviderTimelineRow,
    body: AgentJournalItemBody,
    options: { producer?: AgentJournalProducerLinkage; outlivesTurn?: boolean }
  ): void {
    const known = this.items.get(row.itemId)
    const outlivesTurn = options.outlivesTurn ?? known?.outlivesTurn ?? false
    const obligation = change !== 'close' && (requiresTerminalSettlement(body) || outlivesTurn)
    const bytes = providerTimelineEntryBytes(row.itemId, body)
    if (obligation && !this.budget.admits(row.itemId, bytes)) {
      plan.refuse(PROVIDER_TIMELINE_OVER_BUDGET)
      return
    }
    const ours = known !== undefined
    plan.step(
      {
        kind: 'item',
        reservedBytes: estimateStructuredAgentSessionItemBytes(row.identity, body),
        resolve: (journal) => {
          const current = journal.itemBody(row.itemId)
          const replayed = current !== null && !ours && change === 'open'
          const regresses =
            settledTool(current) && (runningTool(body) ? change !== 'open' || !ours : !ours)
          return replayed || regresses ? null : { identity: row.identity, body }
        },
        options: { ...options.producer, turnScope: row.scope }
      },
      { lifecycle: change !== 'update' }
    )
    plan.commit(() => {
      if (obligation) {
        this.budget.hold(row.itemId, bytes)
      } else {
        this.budget.release(row.itemId)
      }
      const closed = change === 'close' || (change === 'update' && known?.closed === true)
      this.track(row.itemId, { row, closed, obligation, outlivesTurn })
    })
  }

  /** The incarnation a request event names, and whether it is new work. */
  judgeRequest(
    key: string,
    rowFor: (incarnation: number) => ProviderTimelineRow,
    journal: StructuredAgentSessionTransitionJournal | null
  ):
    | { open: ProviderTimelineRow }
    | { adopt: ProviderTimelineRow }
    | { dropped: 'request-duplicate' | 'request-replayed' } {
    const known = this.requests.get(key)
    if (known) {
      const held = journal?.itemBody(known.row.itemId)
      // Pending here and in the journal (or not landed yet): the same request again.
      if (
        known.pending &&
        (held === null || held === undefined || settledProviderTimelineBody(held))
      ) {
        return { dropped: 'request-duplicate' }
      }
      return { open: rowFor(known.incarnation + 1) }
    }
    const first = rowFor(1)
    const held = journal?.itemBody(first.itemId) ?? null
    if (!held) {
      return { open: first }
    }
    return settledProviderTimelineBody(held) ? { adopt: first } : { dropped: 'request-replayed' }
  }

  planRequest(
    plan: ProviderTimelinePlan,
    row: ProviderTimelineRow,
    body: AgentJournalItemBody | null,
    key: string,
    producer?: AgentJournalProducerLinkage
  ): void {
    const bytes = providerTimelineEntryBytes(row.itemId, key)
    if (!this.budget.admits(row.itemId, bytes)) {
      plan.refuse(PROVIDER_TIMELINE_OVER_BUDGET)
      return
    }
    if (body) {
      plan.step(
        {
          kind: 'item',
          reservedBytes: estimateStructuredAgentSessionItemBytes(row.identity, body),
          // A row already there is a client's answer or a replay; neither is overwritten.
          resolve: (journal) =>
            journal.itemBody(row.itemId) === null ? { identity: row.identity, body } : null,
          options: { ...producer, turnScope: row.scope, lifecycle: true }
        },
        { lifecycle: true }
      )
    }
    plan.commit(() => {
      this.budget.hold(row.itemId, bytes)
      this.requests.delete(key)
      this.requests.set(key, { row, incarnation: row.address.incarnation, pending: true })
      this.trimRequests()
    })
  }

  /** The pending request a withdrawal names: this assembler's, else one the journal holds pending. */
  pendingRequest(
    key: string,
    rowFor: (incarnation: number) => ProviderTimelineRow,
    journal: StructuredAgentSessionTransitionJournal | null
  ): ProviderTimelineRow | null {
    const known = this.requests.get(key)
    if (known) {
      return known.pending ? known.row : null
    }
    const first = rowFor(1)
    const held = journal?.itemBody(first.itemId)
    return held && settledProviderTimelineBody(held) ? first : null
  }

  /** The request settled, by withdrawal; its key's next request is a new incarnation. */
  commitWithdrawn(key: string, row: ProviderTimelineRow): void {
    this.budget.release(row.itemId)
    this.requests.set(key, { row, incarnation: row.address.incarnation, pending: false })
  }

  /** Rows of this turn that stay open past it; the turn's settlement leaves them. */
  outlivingTurn(): Set<string> {
    const outliving = new Set<string>()
    for (const [itemId, item] of this.items) {
      if (item.obligation && item.outlivesTurn) {
        outliving.add(itemId)
      }
    }
    return outliving
  }

  /** The turn's settlement was admitted: everything it covered is settled in memory too. */
  commitTurnEnd(turnItemId: string): void {
    for (const [itemId, item] of this.items) {
      if (item.obligation && !item.outlivesTurn && covers(item.row, turnItemId)) {
        this.budget.release(itemId)
        this.track(itemId, { ...item, closed: true, obligation: false })
      }
    }
    for (const request of this.requests.values()) {
      if (request.pending && covers(request.row, turnItemId)) {
        this.budget.release(request.row.itemId)
        request.pending = false
      }
    }
  }

  hasOpenWork(): boolean {
    return (
      [...this.items.values()].some((item) => item.obligation) ||
      [...this.requests.values()].some((request) => request.pending)
    )
  }

  reset(): void {
    for (const itemId of this.items.keys()) {
      this.budget.release(itemId)
    }
    for (const request of this.requests.values()) {
      this.budget.release(request.row.itemId)
    }
    this.items.clear()
    this.requests.clear()
  }

  private track(itemId: string, item: Tracked): void {
    this.items.delete(itemId)
    this.items.set(itemId, item)
    // Only entries no settlement waits on are forgotten; the budget bounds the rest.
    for (const [oldest, each] of this.items) {
      if (this.items.size <= MAX_SETTLED_KEYS + this.budget.size) {
        break
      }
      if (!each.obligation) {
        this.items.delete(oldest)
      }
    }
  }

  private trimRequests(): void {
    for (const [oldest, each] of this.requests) {
      if (this.requests.size <= MAX_SETTLED_KEYS + this.budget.size) {
        break
      }
      if (!each.pending) {
        this.requests.delete(oldest)
      }
    }
  }
}

function covers(row: ProviderTimelineRow, turnItemId: string): boolean {
  return row.scope.kind === 'turn' && row.scope.turnItemId === turnItemId
}
