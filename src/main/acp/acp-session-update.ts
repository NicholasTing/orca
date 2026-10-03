import type { ProviderTimelineEvent } from '../native-chat/agent-session-timeline/provider-timeline-event'
import {
  boundInlineText,
  DEFAULT_JOURNAL_PAYLOAD_LIMITS
} from '../native-chat/agent-session-journal/journal-payload-bounds'
import { acpWindowUsage } from './acp-context-usage'
import type { AcpDialect } from './acp-dialects/acp-dialect'
import type { AcpToolTimeline } from './acp-tool-timeline'
import type { SessionNotification } from './generated/acp-protocol.generated'

export function acpSessionUpdate(
  notification: SessionNotification,
  turn: string | undefined,
  at: number,
  replay: boolean,
  tools: AcpToolTimeline,
  dialect: AcpDialect
): ProviderTimelineEvent[] {
  const update = notification.update
  const join = turn === undefined ? {} : { join: { turn } }
  switch (update.sessionUpdate) {
    case 'agent_message_chunk':
    case 'agent_thought_chunk':
      return update.content.type === 'text'
        ? [
            {
              type: 'text.delta',
              item: update.messageId
                ? { id: `message:${update.messageId}` }
                : { stream: update.sessionUpdate },
              channel: update.sessionUpdate === 'agent_thought_chunk' ? 'reasoning' : 'assistant',
              text: update.content.text,
              ...join
            }
          ]
        : [{ type: 'provider.frame', frameKind: update.sessionUpdate, payload: update, ...join }]
    case 'user_message_chunk':
      return replay && update.content.type === 'text'
        ? [
            {
              type: 'item.update',
              item: `replay-user:${turn}:${update.messageId ?? ''}`,
              body: {
                kind: 'message',
                role: 'user',
                blocks: [{ type: 'text', text: update.content.text }]
              },
              ...join
            }
          ]
        : []
    case 'tool_call':
    case 'tool_call_update':
      return tools.translate(update, dialect, turn)
    case 'plan':
      return [
        {
          type: 'item.update',
          item: `plan:${turn ?? 'thread'}`,
          body: {
            kind: 'status',
            presentation: 'plan-document',
            text: boundInlineText(
              update.entries
                .map(
                  (entry) =>
                    `- [${entry.status === 'completed' ? 'x' : entry.status === 'in_progress' ? '~' : ' '}] ${entry.content}`
                )
                .join('\n'),
              DEFAULT_JOURNAL_PAYLOAD_LIMITS
            ).text
          },
          ...join
        }
      ]
    case 'usage_update':
      return [{ type: 'context.usage', usage: acpWindowUsage(update, at), ...join }]
    case 'available_commands_update':
    case 'current_mode_update':
    case 'config_option_update':
    case 'session_info_update':
      // These feed commands, options and the session title, outside the timeline.
      return []
    case 'plan_update':
    case 'plan_removed':
    case 'compaction_update':
    case 'compaction_summary_chunk':
      return [{ type: 'provider.frame', frameKind: update.sessionUpdate, payload: update, ...join }]
  }
}
