import { z } from 'zod'
import { acpNotificationEnvelopeSchema, tokenEstimate } from '../acp-context-usage'
import type { AcpDialect } from './acp-dialect'
import { grokRequest } from './grok-requests'

const tokenCount = z.number().int().nonnegative()
const usageSchema = z.looseObject({
  inputTokens: tokenCount,
  outputTokens: tokenCount.default(0),
  cachedReadTokens: tokenCount.default(0),
  cacheCreationTokens: tokenCount.default(0)
})
const promptUsageSchema = z.object({ _meta: z.looseObject({ usage: usageSchema.optional() }) })
const toolMetaSchema = z.object({ 'x.ai/tool': z.object({ name: z.string().min(1) }) })
const turnMetaSchema = z.looseObject({
  promptId: z.string().optional(),
  turnStartMs: tokenCount.optional()
})
const completionSchema = z.looseObject({
  sessionUpdate: z.literal('turn_completed'),
  prompt_id: z.string(),
  stop_reason: z.string(),
  usage: usageSchema.optional(),
  elapsed_ms: tokenCount.optional()
})

function usageFrom(value: unknown, at: number) {
  const parsed = usageSchema.safeParse(value)
  if (!parsed.success) {
    return undefined
  }
  const usage = parsed.data
  return tokenEstimate(
    usage.inputTokens,
    usage.outputTokens,
    usage.cachedReadTokens,
    usage.cacheCreationTokens,
    at
  )
}

export const GROK_ACP_DIALECT: AcpDialect = {
  toolName: (update) => {
    const parsed = toolMetaSchema.safeParse(update._meta)
    return parsed.success ? parsed.data['x.ai/tool'].name : undefined
  },
  request: grokRequest,
  promptUsage: (result, at) => {
    const parsed = promptUsageSchema.safeParse(result)
    return parsed.success ? usageFrom(parsed.data._meta.usage ?? parsed.data._meta, at) : undefined
  },
  notification: (method, params, at) => {
    if (
      !['session/update', '_x.ai/session_notification', '_x.ai/session/update'].includes(method)
    ) {
      return undefined
    }
    const parsed = acpNotificationEnvelopeSchema.safeParse(params)
    if (!parsed.success) {
      return undefined
    }
    const envelope = parsed.data
    const meta = turnMetaSchema.safeParse(envelope._meta)
    const completion = completionSchema.safeParse(envelope.update)
    const turn = completion.success
      ? completion.data.prompt_id
      : meta.success
        ? meta.data.promptId
        : undefined
    const usage = completion.success ? usageFrom(completion.data.usage, at) : undefined
    return {
      ...(turn === undefined ? {} : { turn, agentInitiated: turn.startsWith('task-completed-') }),
      ...(meta.success && meta.data.turnStartMs !== undefined ? { at: meta.data.turnStartMs } : {}),
      replay: envelope._meta?.isReplay === true,
      ...(completion.success
        ? {
            end: {
              stopReason: completion.data.stop_reason,
              ...(completion.data.elapsed_ms === undefined
                ? {}
                : { durationMs: completion.data.elapsed_ms })
            }
          }
        : {}),
      ...(usage === undefined ? {} : { usage })
    }
  }
}
