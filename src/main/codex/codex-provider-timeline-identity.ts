// Codex's persisted identities, spelled for the shared timeline assembler.
//
// The same rows Codex writes today (`codexItemIdentity`, `codexTurnLifecycleIdentity`): a user or
// assistant message inside a turn keys by (thread, turn, message ordinal), which survives a resume
// because the ordinal counts messages only; anything else keys by its Codex item id; a turn keys by
// its Codex turn id. Codex ids name one conversation for good, so the namespace spells nothing.

import { agentJournalItemKey } from '../../shared/agent-session-journal-item-key'
import type { ProviderTimelineIdentityScheme } from '../native-chat/agent-session-timeline/provider-timeline-identity'
import {
  codexTurnLifecycleIdentity,
  codexTurnUserItemId
} from './codex-structured-journal-translation-turns'

export function createCodexProviderTimelineIdentityScheme(input: {
  sessionId: string
  primaryThreadId: () => string | null
}): ProviderTimelineIdentityScheme {
  const turn: ProviderTimelineIdentityScheme['turn'] = (address) =>
    codexTurnLifecycleIdentity(input.sessionId, address.key.value)
  return {
    turn,
    turnId: (address) => address.key.value,
    turnOpener: (address) => {
      const thread = input.primaryThreadId()
      return thread === null
        ? agentJournalItemKey(turn(address))
        : codexTurnUserItemId(thread, address.key.value)
    },
    item: (address) => {
      const threadId = address.thread ?? input.primaryThreadId() ?? ''
      return address.itemClass === 'message' && address.turn && address.messageOrdinal !== null
        ? {
            provider: 'codex',
            threadId,
            turnId: address.turn.value,
            ordinal: address.messageOrdinal
          }
        : { provider: 'orca', clientMessageId: `codex-item:${threadId}:${address.key.value}` }
    }
  }
}
