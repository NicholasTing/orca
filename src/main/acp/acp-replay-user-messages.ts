import type { AgentJournalMessageItem } from '../../shared/agent-session-journal-types'
import {
  boundInlineText,
  DEFAULT_JOURNAL_PAYLOAD_LIMITS
} from '../native-chat/agent-session-journal/journal-payload-bounds'
import type { SessionUpdate } from './generated/acp-protocol.generated'

/** User text has no grammar stream channel; adopted history needs a bounded whole-body snapshot. */
export class AcpReplayUserMessages {
  private current?: { id: string | null; text: string }

  observe(update: SessionUpdate): { startsMessage: boolean; body?: AgentJournalMessageItem } {
    if (update.sessionUpdate !== 'user_message_chunk') {
      if (
        [
          'agent_message_chunk',
          'agent_thought_chunk',
          'tool_call',
          'tool_call_update',
          'plan'
        ].includes(update.sessionUpdate)
      ) {
        this.current = undefined
      }
      return { startsMessage: false }
    }
    const id = update.messageId ?? null
    const startsMessage = !this.current || this.current.id !== id
    const previous = startsMessage ? '' : this.current!.text
    if (update.content.type !== 'text') {
      return { startsMessage }
    }
    const text = boundInlineText(
      previous + update.content.text,
      DEFAULT_JOURNAL_PAYLOAD_LIMITS
    ).text
    this.current = { id, text }
    return {
      startsMessage,
      body: { kind: 'message', role: 'user', blocks: [{ type: 'text', text }] }
    }
  }
}
