import type { AgentSessionContextUsage } from '../../../shared/agent-session-context-usage'
import type { AgentSessionPromptResponse } from '../../../shared/agent-session-question-answer'
import type { ProviderTimelineRequestBody } from '../../native-chat/agent-session-timeline/provider-timeline-event'
import type { ToolCallUpdate } from '../generated/acp-protocol.generated'

export type AcpRequestPresentation = {
  body: ProviderTimelineRequestBody
  reply(response: AgentSessionPromptResponse | null): unknown
}

export type AcpDialectNotification = {
  turn?: string
  agentInitiated?: boolean
  replay?: boolean
  at?: number
  end?: { stopReason: string; durationMs?: number }
  usage?: AgentSessionContextUsage
}

/** Hooks only interpret provider extensions; lifecycle and row identity stay shared. */
export type AcpDialect = {
  toolName?(update: ToolCallUpdate): string | undefined
  notification?(method: string, params: unknown, at: number): AcpDialectNotification | undefined
  promptUsage?(result: unknown, at: number): AgentSessionContextUsage | undefined
  request?(method: string, params: unknown): AcpRequestPresentation | undefined
}

export const GENERIC_ACP_DIALECT: AcpDialect = {}
