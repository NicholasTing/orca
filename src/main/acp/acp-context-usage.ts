import { z } from 'zod'
import type { AgentSessionContextUsage } from '../../shared/agent-session-context-usage'
import type { Usage, UsageUpdate } from './generated/acp-protocol.generated'

export function acpWindowUsage(update: UsageUpdate, at: number): AgentSessionContextUsage {
  return {
    window: { tokens: update.size, capturedAt: at },
    used: {
      kind: 'estimate',
      capturedAt: at,
      usage: {
        inputTokens: update.used,
        cacheReadInputTokens: 0,
        cacheCreationInputTokens: 0,
        outputTokens: 0
      }
    }
  }
}

export function acpResponseUsage(usage: Usage, at: number): AgentSessionContextUsage {
  return tokenEstimate(
    usage.inputTokens,
    usage.outputTokens,
    usage.cachedReadTokens ?? 0,
    usage.cachedWriteTokens ?? 0,
    at
  )
}

export function tokenEstimate(
  input: number,
  output: number,
  cachedRead: number,
  cachedWrite: number,
  at: number
): AgentSessionContextUsage {
  return {
    used: {
      kind: 'estimate',
      capturedAt: at,
      usage: {
        // ACP input includes cached tokens; the shared estimate adds its cache fields.
        inputTokens: Math.max(0, input - cachedRead - cachedWrite),
        cacheReadInputTokens: cachedRead,
        cacheCreationInputTokens: cachedWrite,
        outputTokens: output
      }
    }
  }
}

export const acpNotificationEnvelopeSchema = z.looseObject({
  sessionId: z.string(),
  update: z.looseObject({ sessionUpdate: z.string() }),
  _meta: z.looseObject({ isReplay: z.boolean().optional() }).nullish()
})
