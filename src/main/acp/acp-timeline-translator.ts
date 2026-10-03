import { z } from 'zod'
import type { AgentJournalRenderItem } from '../../shared/agent-session-journal-types'
import type { ProviderTimelineEvent } from '../native-chat/agent-session-timeline/provider-timeline-event'
import { acpNotificationEnvelopeSchema, acpResponseUsage } from './acp-context-usage'
import {
  GENERIC_ACP_DIALECT,
  type AcpDialect,
  type AcpDialectNotification,
  type AcpRequestPresentation
} from './acp-dialects/acp-dialect'
import { AcpReplayUserMessages } from './acp-replay-user-messages'
import { AcpRpcError } from './acp-errors'
import type { AcpSessionEvent } from './acp-session-runtime'
import { acpPermissionPresentation } from './acp-timeline-requests'
import { acpSessionUpdate } from './acp-session-update'
import { AcpToolTimeline } from './acp-tool-timeline'
import { SessionNotificationSchema, type PromptResponse } from './generated/acp-protocol.generated'

const requestSessionSchema = z.object({ sessionId: z.string() })

export function acpTurnEnd(
  turn: string,
  stopReason: string,
  at: number,
  durationMs?: number
): ProviderTimelineEvent {
  return {
    type: 'turn.end',
    turn,
    at,
    state: stopReason === 'cancelled' ? 'interrupted' : 'completed',
    ...(stopReason === 'end_turn'
      ? { outcome: 'success' as const }
      : stopReason === 'cancelled'
        ? { outcome: 'cancellation' as const }
        : ['refusal', 'max_tokens', 'max_turn_requests'].includes(stopReason)
          ? { outcome: 'failure' as const }
          : {}),
    ...(durationMs === undefined ? {} : { durationMs })
  }
}

type PromptTurn = { turn: string; providerTurn?: string; durationMs?: number }
type LoadReplay = { adopt: boolean; turn?: string; serial: number; users: AcpReplayUserMessages }

export type AcpTimelineTranslatorOptions = {
  sessionId: string
  /** The host supplies the committed journal after draining earlier writes, before session/load. */
  journalItems(): readonly AgentJournalRenderItem[]
  dialect?: AcpDialect
}

/** Each method consumes one provider frame once; the lane retries returned grammar events. */
export class AcpTimelineTranslator {
  private readonly dialect: AcpDialect
  private readonly tools = new AcpToolTimeline()
  private readonly aliases = new Map<string, string>()
  private prompt?: PromptTurn
  private replay?: LoadReplay

  constructor(private readonly options: AcpTimelineTranslatorOptions) {
    this.dialect = options.dialect ?? GENERIC_ACP_DIALECT
  }

  openPrompt(clientMessageId: string, at: number): ProviderTimelineEvent[] {
    if (this.prompt || this.replay) {
      throw new Error('ACP prompt overlaps a prompt or load')
    }
    const turn = `prompt:${clientMessageId}`
    this.prompt = { turn }
    return [
      { type: 'input.accepted', clientMessageId, requestedAt: at, join: { turn } },
      { type: 'turn.open', turn, at }
    ]
  }

  promptResult(
    clientMessageId: string,
    result: PromptResponse,
    at: number
  ): ProviderTimelineEvent[] {
    const turn = `prompt:${clientMessageId}`
    const usage =
      this.dialect.promptUsage?.(result, at) ??
      (result.usage ? acpResponseUsage(result.usage, at) : undefined)
    const events: ProviderTimelineEvent[] = usage
      ? [{ type: 'context.usage', usage, join: { turn } }]
      : []
    if (!['end_turn', 'cancelled'].includes(result.stopReason)) {
      events.push({
        type: 'provider.frame',
        frameKind: `prompt:${result.stopReason}`,
        payload: result,
        join: { turn }
      })
    }
    events.push(
      acpTurnEnd(
        turn,
        result.stopReason,
        at,
        this.prompt?.turn === turn ? this.prompt.durationMs : undefined
      )
    )
    this.tools.end(turn)
    if (this.prompt?.turn === turn) {
      this.prompt = undefined
    }
    return events
  }

  beginLoad(): void {
    if (this.prompt || this.replay) {
      throw new Error('ACP load overlaps a prompt or load')
    }
    this.replay = {
      adopt: this.options.journalItems().length === 0,
      serial: 0,
      users: new AcpReplayUserMessages()
    }
  }

  finishLoad(at: number): ProviderTimelineEvent[] {
    const turn = this.replay?.turn
    this.replay = undefined
    if (!turn) {
      return []
    }
    this.tools.end(turn)
    // Generic ACP history has no stop verdict; load completion must not invent success.
    return [{ type: 'turn.end', turn, at, state: 'completed' }]
  }

  sessionEvent(event: AcpSessionEvent, at: number): ProviderTimelineEvent[] {
    return this.notification(
      'session/update',
      event.kind === 'known' ? event.notification : event.raw,
      at
    )
  }

  notification(method: string, params: unknown, at: number): ProviderTimelineEvent[] {
    const envelope = acpNotificationEnvelopeSchema.safeParse(params)
    const session = requestSessionSchema.safeParse(params)
    if (session.success && session.data.sessionId !== this.options.sessionId) {
      return []
    }
    const extension = this.dialect.notification?.(method, params, at)
    const markedReplay =
      (envelope.success && envelope.data._meta?.isReplay === true) || extension?.replay === true
    if (markedReplay && !this.replay) {
      throw new Error('ACP replay requires beginLoad journal decision')
    }
    const isReplay = extension?.replay ?? (markedReplay || this.replay !== undefined)
    if (isReplay && this.replay && !this.replay.adopt) {
      return []
    }
    const standard =
      method === 'session/update' ? SessionNotificationSchema.safeParse(params) : undefined
    const events: ProviderTimelineEvent[] = []
    const substantive =
      standard?.success &&
      [
        'user_message_chunk',
        'agent_message_chunk',
        'agent_thought_chunk',
        'tool_call',
        'tool_call_update',
        'plan'
      ].includes(standard.data.update.sessionUpdate)
    const replayUser =
      isReplay && this.replay?.adopt && standard?.success
        ? this.replay.users.observe(standard.data.update)
        : undefined
    if (replayUser?.startsMessage && this.replay?.turn) {
      events.push({ type: 'turn.end', turn: this.replay.turn, at, state: 'completed' })
      this.tools.end(this.replay.turn)
      this.replay.turn = undefined
    }
    const turn = this.routeTurn(
      extension,
      events,
      at,
      substantive || extension?.end !== undefined,
      isReplay
    )
    const join = turn === undefined ? {} : { join: { turn } }
    if (extension?.usage) {
      events.push({ type: 'context.usage', usage: extension.usage, ...join })
    }
    if (extension?.end && turn) {
      // Client prompts settle on their own response; extension completions own autonomous turns.
      if (this.prompt?.turn === turn) {
        this.prompt.durationMs = extension.end.durationMs
      } else {
        if (!['end_turn', 'cancelled'].includes(extension.end.stopReason)) {
          events.push({
            type: 'provider.frame',
            frameKind: `turn:${extension.end.stopReason}`,
            payload: params,
            ...join
          })
        }
        events.push(acpTurnEnd(turn, extension.end.stopReason, at, extension.end.durationMs))
        this.tools.end(turn)
        if (this.replay?.turn === turn) {
          this.replay.turn = undefined
        }
      }
      return events
    }
    if (standard?.success) {
      return [
        ...events,
        ...acpSessionUpdate(
          standard.data,
          turn,
          at,
          isReplay && (this.replay?.adopt ?? false),
          this.tools,
          this.dialect,
          replayUser?.body
        )
      ]
    }
    events.push({ type: 'provider.frame', frameKind: method, payload: params, ...join })
    return events
  }

  request(
    method: string,
    params: unknown,
    id: string | number
  ): {
    events: ProviderTimelineEvent[]
    presentation?: AcpRequestPresentation
  } {
    const session = requestSessionSchema.safeParse(params)
    if (!session.success || session.data.sessionId !== this.options.sessionId) {
      throw new AcpRpcError(-32602, 'ACP request belongs to an unknown session')
    }
    const presentation =
      method === 'session/request_permission'
        ? acpPermissionPresentation(params)
        : this.dialect.request?.(method, params)
    if (!presentation) {
      return {
        events: [{ type: 'provider.frame', frameKind: `request:${method}`, payload: params }]
      }
    }
    return {
      presentation,
      events: [
        {
          type: 'request.open',
          request: `${method}:${JSON.stringify(id)}`,
          body: presentation.body
        }
      ]
    }
  }

  private alias(providerTurn: string, turn: string): void {
    if (this.aliases.size >= 128) {
      this.aliases.delete(this.aliases.keys().next().value ?? '')
    }
    this.aliases.set(providerTurn, turn)
  }

  private routeTurn(
    extension: AcpDialectNotification | undefined,
    events: ProviderTimelineEvent[],
    at: number,
    substantive: boolean,
    isReplay: boolean
  ): string | undefined {
    const providerTurn = extension?.turn
    const known = providerTurn ? this.aliases.get(providerTurn) : undefined
    if (known) {
      return known
    }
    if (isReplay && this.replay?.adopt && substantive) {
      if (!this.replay.turn) {
        this.replay.turn = providerTurn ?? `replay:${this.replay.serial++}`
        events.push({ type: 'turn.open', turn: this.replay.turn, at: extension?.at ?? at })
      }
      if (providerTurn) {
        this.alias(providerTurn, this.replay.turn)
      }
      return this.replay.turn
    }
    if (extension?.agentInitiated && providerTurn && substantive) {
      this.alias(providerTurn, providerTurn)
      events.push({ type: 'turn.open', turn: providerTurn, at: extension.at ?? at })
      return providerTurn
    }
    if (providerTurn && this.prompt && !this.prompt.providerTurn) {
      this.prompt.providerTurn = providerTurn
      this.alias(providerTurn, this.prompt.turn)
    }
    return (
      (providerTurn ? (this.aliases.get(providerTurn) ?? providerTurn) : undefined) ??
      this.replay?.turn ??
      this.prompt?.turn
    )
  }
}
