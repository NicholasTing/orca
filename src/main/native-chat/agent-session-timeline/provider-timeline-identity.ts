// How the assembler's addresses become persisted journal identities.
//
// The assembler decides WHICH rows are the same row: it resolves every provider join to an
// address — the provider session's namespace, the key, the thread and turn the item belongs to,
// the item's class, and its place among its turn's messages — or mints a key when the provider
// names nothing. A scheme only spells an address as an `AgentJournalItemIdentity`. It must be pure:
// revisions find their row by recomputing it. It is the one place a provider's persisted identity
// shape lives, so moving a provider to a new identity arm is a change here.

import type {
  AgentJournalItemBody,
  AgentJournalItemIdentity,
  AgentType
} from '../../../shared/agent-session-journal-types'
import { boundPayload, digestPayload } from '../agent-session-journal/journal-payload-bounds'

/** A key the provider vouched for, or one the assembler minted because it named nothing.
 *  A minted value is unique per acquisition, so it never needs a namespace. */
export type ProviderTimelineKey = { source: 'provider' | 'minted'; value: string }

export type ProviderTimelineTurnAddress = {
  /** The provider session whose ids the key belongs to. */
  namespace: string
  key: ProviderTimelineKey
}

/** Which family an item belongs to; families never share a key space. Streamed text and full
 *  item snapshots are one family, so a provider-named message is one row however it arrives. */
export type ProviderTimelineItemFamily = 'item' | 'request' | 'frame'

/** What kind of thing an item is, derived from what it carries: `message` is user or assistant
 *  prose; every other class is its row kind (`reasoning` text included). */
export type ProviderTimelineItemClass =
  | 'message'
  | 'reasoning'
  | Exclude<AgentJournalItemBody['kind'], 'message'>
  | 'frame'

export type ProviderTimelineItemAddress = {
  namespace: string
  family: ProviderTimelineItemFamily
  key: ProviderTimelineKey
  /** The provider thread it came from; null when the provider named none. */
  thread: string | null
  /** The provider turn it belongs to; null for a row outside any turn. */
  turn: ProviderTimelineKey | null
  itemClass: ProviderTimelineItemClass
  /** Its place among its (thread, turn)'s `message` items, by first sight; null for any other. */
  messageOrdinal: number | null
  /** 1 for the first request under a key; a key reused after it settled is the next one. */
  incarnation: number
}

export type ProviderTimelineIdentityScheme = {
  turn(address: ProviderTimelineTurnAddress): AgentJournalItemIdentity
  /** The id the turn row carries and a client's Stop names. */
  turnId(address: ProviderTimelineTurnAddress): string
  /** The user item a turn names when no send of Orca's opened it; absent ⇒ the turn row itself. */
  turnOpener?(address: ProviderTimelineTurnAddress): string
  item(address: ProviderTimelineItemAddress): AgentJournalItemIdentity
}

export function providerTimelineItemClass(body: AgentJournalItemBody): ProviderTimelineItemClass {
  if (body.kind !== 'message') {
    return body.kind
  }
  return body.role === 'user' || body.role === 'assistant' ? 'message' : 'reasoning'
}

const MAX_KEY_PART_BYTES = 256

/** A provider id as a bounded identity part: escaped so `:` cannot forge another part, and
 *  digest-suffixed when long so two long ids never share a prefix-only spelling. */
export function providerTimelineKeyPart(value: string): string {
  const encoded = encodeURIComponent(value)
  if (Buffer.byteLength(encoded, 'utf8') <= MAX_KEY_PART_BYTES) {
    return encoded
  }
  const suffix = `#${digestPayload(value).slice(0, 24)}`
  const head = boundPayload(encoded, {
    inlineHeadBytes: MAX_KEY_PART_BYTES - Buffer.byteLength(suffix, 'utf8')
  }).head
  return `${head}${suffix}`
}

function spell(namespace: string, key: ProviderTimelineKey, thread: string | null = null): string {
  // A provider's item ids are its own per thread, so a subagent thread's ids are spelled apart.
  const scope = thread === null ? '' : `${providerTimelineKeyPart(thread)}/`
  return key.source === 'provider'
    ? `p:${providerTimelineKeyPart(namespace)}:${scope}${providerTimelineKeyPart(key.value)}`
    : `m:${providerTimelineKeyPart(key.value)}`
}

/**
 * The scheme for a provider with no identity arm of its own: the existing `legacy` arm, so no row
 * shape changes. Turn rows keep the `turn-lifecycle:` record prefix the other lanes write.
 * Provider keys are spelled inside their namespace, and apart from minted ones.
 */
export function createLegacyProviderTimelineIdentityScheme(input: {
  agent: AgentType
  sessionId: string
}): ProviderTimelineIdentityScheme {
  const identity = (recordId: string): AgentJournalItemIdentity => ({
    provider: 'legacy',
    agent: input.agent,
    sessionId: input.sessionId,
    recordId
  })
  return {
    turn: (address) => identity(`turn-lifecycle:${spell(address.namespace, address.key)}`),
    turnId: (address) => spell(address.namespace, address.key),
    item: (address) =>
      identity(
        `${address.family}:${spell(address.namespace, address.key, address.family === 'request' ? null : address.thread)}${
          address.incarnation > 1 ? `#${address.incarnation}` : ''
        }`
      )
  }
}
