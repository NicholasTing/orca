// The narrow grammar a provider adapter speaks to the timeline assembler.
//
// The adapter knows the provider's dialect; the assembler knows the timeline. An adapter parses
// provider traffic into these semantic events and never writes a journal row or mints a journal
// identity: every key here is the provider's own id (a turn, a tool call, a message, a thread),
// or a name the adapter keeps stable for something the provider does not name. The assembler
// resolves those joins to rows, scopes rows to turns, and owns the turn and item rules in
// `agent-session-journal-types.ts`; the journal decides, at each write, what a replay changes.
//
// Rules an adapter can rely on:
// - Only `turn.open` opens a turn. Items, text and requests never do: one that arrives with no
//   turn open and names none is written as a thread row. An adapter whose provider starts work on
//   its own (a background wake, auto-compaction) decides that a turn began and says `turn.open`
//   before that work's items.
// - `join.turn` attaches a row to the turn the provider says it belongs to, open or not.
// - A turn settles once, with only the provider's verdict; the journal refuses a second settlement
//   and a replayed open, even after a restart.
// - An event the sink refused changed nothing. Re-apply the same event to retry it (after
//   `backpressure`); `failed` and `closed` are final.
// - After `session.ended` every event is dropped until `session.reset`. Reset whenever the
//   provider's ids may repeat (a new process): its ids then name new rows.

import type { AgentSessionContextUsage } from '../../../shared/agent-session-context-usage'
import type {
  AgentJournalApprovalItem,
  AgentJournalItemBody,
  AgentJournalProducerLinkage,
  AgentJournalQuestionItem,
  AgentJournalTurnOutcome
} from '../../../shared/agent-session-journal-types'
import type { StructuredAgentSessionTurnVerdict } from '../agent-session-wire/structured-agent-session-stale-turn-verdict'

/** Bodies an item event may carry. Turn rows are the assembler's; prompts travel as requests. */
export type ProviderTimelineItemBody = Exclude<
  AgentJournalItemBody,
  { kind: 'turn' | 'approval' | 'question' }
>

export type ProviderTimelineRequestBody = AgentJournalApprovalItem | AgentJournalQuestionItem

/** `assistant` is reply text; `reasoning` is the model's visible thinking. */
export type ProviderTimelineTextChannel = 'assistant' | 'reasoning'

/** Where the provider says something belongs. */
export type ProviderTimelineJoin = {
  /** The provider thread it came from, when the provider runs several (a subagent's own thread). */
  thread?: string
  /** The provider turn it belongs to. Absent: the turn open now. */
  turn?: string
}

/** A streamed text item: one the provider names (the same item as item events with that id), or
 *  an anonymous stream that becomes a new message each time it starts. */
export type ProviderTimelineTextItem = { id: string } | { stream: string }

type Produced = {
  /** The subagent that produced this, when not the session's own agent. Stamped on the row as is. */
  producer?: AgentJournalProducerLinkage
}

type Joined = { join?: ProviderTimelineJoin }

export type ProviderTimelineEvent =
  /** Orca's send reached the provider. It names the open turn when nothing opened that one, else
   *  the next. `join.item`: the provider's id for its echo of the send, which takes that message's
   *  place among its turn's messages and writes no row (the send's own row is the bubble). */
  | {
      type: 'input.accepted'
      clientMessageId: string
      requestedAt: number
      join?: ProviderTimelineJoin & { item?: string }
    }
  /** A turn began. `turn` is the provider's turn id when it has one; the assembler mints one otherwise. */
  | { type: 'turn.open'; turn?: string; at: number }
  /** The provider ended a turn. Absent `turn` means the open turn. */
  | {
      type: 'turn.end'
      turn?: string
      at: number
      state: 'completed' | 'interrupted'
      /** The provider's own verdict. Absent means it gave none, which reads as unknown. */
      outcome?: AgentJournalTurnOutcome
      /** The provider's own measured duration. */
      durationMs?: number
    }
  /** Work began. `outlivesTurn` keeps it open past its turn's end (a backgrounded task). */
  | ({
      type: 'item.open'
      item: string
      body: ProviderTimelineItemBody
      outlivesTurn?: true
    } & Produced &
      Joined)
  /** The item's whole current body. Content may be replaced; a settled tool never runs again. */
  | ({ type: 'item.update'; item: string; body: ProviderTimelineItemBody } & Produced & Joined)
  /** The item's whole terminal body; it replaces any text streamed into the same item. */
  | ({ type: 'item.close'; item: string; body: ProviderTimelineItemBody } & Produced & Joined)
  /** Streamed text. */
  | ({
      type: 'text.delta'
      item: ProviderTimelineTextItem
      channel: ProviderTimelineTextChannel
      text: string
    } & Produced &
      Joined)
  /** The stream ended; `text` is the provider's final text, else what streamed is kept. */
  | { type: 'text.close'; item: ProviderTimelineTextItem; text?: string }
  /** The provider asked the user something and waits on the answer. */
  | ({ type: 'request.open'; request: string; body: ProviderTimelineRequestBody } & Produced &
      Joined)
  /** The provider stopped waiting for an answer it never got. */
  | { type: 'request.withdrawn'; request: string }
  /** What the provider said about its context window: for `join.turn`, else the open turn, else the last. */
  | ({ type: 'context.usage'; usage: AgentSessionContextUsage } & Joined)
  /** The live activity line for the open turn; null clears it. */
  | { type: 'activity'; text: string | null }
  /** Provider traffic no typed event covers; it becomes the shared bounded fallback row. */
  | ({ type: 'provider.frame'; frameKind: string; payload: unknown } & Joined)
  /** The provider child is gone. The verdict is what the host can prove about its end. */
  | { type: 'session.ended'; verdict: StructuredAgentSessionTurnVerdict }
  /** A new provider session began; its ids live in `namespace`. Work still open is settled first,
   *  as lost (`unverifiable`), since nothing ended it. */
  | { type: 'session.reset'; namespace: string }
