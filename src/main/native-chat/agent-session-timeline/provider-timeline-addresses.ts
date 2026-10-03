// Provider joins → addresses, and addresses → journal rows.
//
// The one place keys are resolved: a namespace per provider session, minted keys unique per
// acquisition, message ordinals per (thread, turn), and a bounded memory of which address and turn
// scope each provider key resolved to, so a later event for the same key reaches the same row in
// the same turn even after another turn opened. Forgetting an entry only means a later event for
// that key resolves afresh; what the row holds is always read from the journal.

import { agentJournalItemKey } from '../../../shared/agent-session-journal-item-key'
import type {
  AgentJournalItemIdentity,
  AgentJournalTurnScope
} from '../../../shared/agent-session-journal-types'
import type {
  ProviderTimelineIdentityScheme,
  ProviderTimelineItemAddress,
  ProviderTimelineItemClass,
  ProviderTimelineItemFamily,
  ProviderTimelineKey,
  ProviderTimelineTurnAddress
} from './provider-timeline-identity'
import { ProviderTurnMessageOrdinals } from './provider-turn-message-ordinals'

/** Resolved keys remembered; a late event past this many resolves afresh. */
const MAX_RESOLVED_KEYS = 1_024

export type ProviderTimelineRow = {
  address: ProviderTimelineItemAddress
  identity: AgentJournalItemIdentity
  itemId: string
  scope: AgentJournalTurnScope
}

export type ProviderTimelineTurnRef = {
  address: ProviderTimelineTurnAddress
  identity: AgentJournalItemIdentity
  itemId: string
  turnId: string
}

export class ProviderTimelineAddresses {
  private readonly ordinals = new ProviderTurnMessageOrdinals()
  private readonly resolved = new Map<string, ProviderTimelineRow>()
  private serial = 0

  constructor(
    private readonly deps: {
      scheme: ProviderTimelineIdentityScheme
      generation: string
      namespace: string
    }
  ) {}

  get namespace(): string {
    return this.deps.namespace
  }

  /** A new provider session: its ids name new rows. Minted keys stay unique as they are. */
  reset(namespace: string): void {
    this.deps.namespace = namespace
    this.resolved.clear()
  }

  /** A key unique to this acquisition. A refused event wastes one; nothing reuses it. */
  mint(kind: string): ProviderTimelineKey {
    this.serial += 1
    return { source: 'minted', value: `${this.deps.generation}:${kind}${this.serial}` }
  }

  turn(key: ProviderTimelineKey): ProviderTimelineTurnRef {
    const address = { namespace: this.deps.namespace, key }
    const identity = this.deps.scheme.turn(address)
    return {
      address,
      identity,
      itemId: agentJournalItemKey(identity),
      turnId: this.deps.scheme.turnId(address)
    }
  }

  /** The user item a turn names when no send of Orca's opened it. */
  turnOpener(turn: ProviderTimelineTurnRef): string {
    return this.deps.scheme.turnOpener?.(turn.address) ?? turn.itemId
  }

  /** The row a provider key already resolved to, if remembered. Item keys are the provider's per
   *  thread; request keys are per session. */
  known(
    family: ProviderTimelineItemFamily,
    key: string,
    options: { incarnation?: number; thread?: string | null } = {}
  ): ProviderTimelineRow | null {
    return (
      this.resolved.get(
        this.memoryKey(family, key, options.incarnation ?? 1, options.thread ?? null)
      ) ?? null
    )
  }

  /** Resolves an item: the remembered row for its key, else a new address in `placement`. */
  row(
    input: {
      family: ProviderTimelineItemFamily
      key: ProviderTimelineKey
      itemClass: ProviderTimelineItemClass
      incarnation?: number
    },
    placement: {
      thread: string | null
      turn: ProviderTimelineKey | null
      scope: AgentJournalTurnScope
    }
  ): ProviderTimelineRow {
    const incarnation = input.incarnation ?? 1
    const remembered =
      input.key.source === 'provider'
        ? this.known(input.family, input.key.value, { incarnation, thread: placement.thread })
        : null
    if (remembered) {
      return remembered
    }
    const address: ProviderTimelineItemAddress = {
      namespace: this.deps.namespace,
      family: input.family,
      key: input.key,
      thread: placement.thread,
      turn: placement.turn,
      itemClass: input.itemClass,
      // Idempotent per key, so a refused event that retries keeps the same place.
      messageOrdinal:
        input.itemClass === 'message' && placement.turn
          ? this.ordinals.ordinalFor(placement.thread ?? '', placement.turn.value, input.key.value)
          : null,
      incarnation
    }
    const identity = this.deps.scheme.item(address)
    return { address, identity, itemId: agentJournalItemKey(identity), scope: placement.scope }
  }

  /** Takes a message's place among its turn's messages without a row (an echoed send). */
  reserveMessage(thread: string | null, turn: ProviderTimelineKey, item: string): void {
    this.ordinals.ordinalFor(thread ?? '', turn.value, item)
  }

  /** The row's key now resolves to it; called when the event that wrote it is admitted. */
  remember(row: ProviderTimelineRow): void {
    if (row.address.key.source !== 'provider') {
      return
    }
    const key = this.memoryKey(
      row.address.family,
      row.address.key.value,
      row.address.incarnation,
      row.address.thread
    )
    this.resolved.delete(key)
    this.resolved.set(key, row)
    for (const oldest of this.resolved.keys()) {
      if (this.resolved.size <= MAX_RESOLVED_KEYS) {
        break
      }
      this.resolved.delete(oldest)
    }
  }

  private memoryKey(
    family: ProviderTimelineItemFamily,
    key: string,
    incarnation: number,
    thread: string | null
  ): string {
    const scope = family === 'request' ? '' : (thread ?? '')
    return `${family}\u001f${incarnation}\u001f${scope}\u001f${key}`
  }
}
