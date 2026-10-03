import { createPatch } from 'diff'
import { z } from 'zod'
import type { AgentJournalToolCallItem } from '../../shared/agent-session-journal-types'
import {
  boundPayload,
  boundToolInput,
  DEFAULT_JOURNAL_PAYLOAD_LIMITS
} from '../native-chat/agent-session-journal/journal-payload-bounds'
import type { ProviderTimelineEvent } from '../native-chat/agent-session-timeline/provider-timeline-event'
import type { AcpDialect } from './acp-dialects/acp-dialect'
import type { ToolCallUpdate } from './generated/acp-protocol.generated'

const rawStreamsSchema = z.object({ stdout: z.string().optional(), stderr: z.string().optional() })

function outputText(update: ToolCallUpdate): string | undefined {
  const text = update.content?.flatMap((block) =>
    block.type === 'content' && block.content.type === 'text' ? [block.content.text] : []
  )
  if (text?.length) {
    return text.join('\n')
  }
  if (update.rawOutput === undefined) {
    return undefined
  }
  if (typeof update.rawOutput === 'string') {
    return update.rawOutput
  }
  const streams = rawStreamsSchema.safeParse(update.rawOutput)
  if (streams.success && (streams.data.stdout !== undefined || streams.data.stderr !== undefined)) {
    return [streams.data.stdout, streams.data.stderr]
      .filter((value) => value !== undefined)
      .join('\n')
  }
  return JSON.stringify(update.rawOutput) ?? ''
}

/** A whole-file replacement is a linear-time unified patch, even for unrelated large files. */
function replacementPatch(path: string, oldText: string, newText: string): string {
  const lines = (text: string, prefix: string): { count: number; text: string } => {
    if (!text) {
      return { count: 0, text: '' }
    }
    const entries = text.split('\n')
    if (entries.at(-1) === '') {
      entries.pop()
    }
    return {
      count: entries.length,
      text:
        entries.map((line) => `${prefix}${line}\n`).join('') +
        (text.endsWith('\n') ? '' : '\\ No newline at end of file\n')
    }
  }
  const oldLines = lines(oldText, '-')
  const newLines = lines(newText, '+')
  return `--- ${path}\n+++ ${path}\n@@ -${oldLines.count ? 1 : 0},${oldLines.count} +${newLines.count ? 1 : 0},${newLines.count} @@\n${oldLines.text}${newLines.text}`
}

function boundedDiffPatch(path: string, oldText: string, newText: string): string {
  const patch =
    oldText.length + newText.length <= 256 * 1024
      ? createPatch(path, oldText, newText, undefined, undefined, { maxEditLength: 1024 })
      : undefined
  return patch ?? replacementPatch(path, oldText, newText)
}

type ToolSnapshot = { body: AgentJournalToolCallItem; turn?: string }

/** Holds only bounded snapshots needed to fill ACP's partial tool updates. */
export class AcpToolTimeline {
  private readonly tools = new Map<string, ToolSnapshot>()
  private bytes = 0

  translate(update: ToolCallUpdate, dialect: AcpDialect, turn?: string): ProviderTimelineEvent[] {
    const previous = this.tools.get(update.toolCallId)
    const output = outputText(update)
    const state =
      update.status === 'completed'
        ? 'completed'
        : update.status === 'failed'
          ? 'failed'
          : update.status === 'pending' || update.status === 'in_progress'
            ? 'running'
            : (previous?.body.state ?? 'running')
    const body: AgentJournalToolCallItem = {
      kind: 'tool-call',
      callId: update.toolCallId,
      name:
        dialect.toolName?.(update) ??
        update.name ??
        previous?.body.name ??
        update.title ??
        update.kind ??
        'tool',
      input:
        update.rawInput === undefined
          ? (previous?.body.input ?? null)
          : boundToolInput(update.rawInput, DEFAULT_JOURNAL_PAYLOAD_LIMITS),
      state,
      ...(output === undefined
        ? previous?.body.output
          ? { output: previous.body.output }
          : {}
        : { output: boundPayload(output, DEFAULT_JOURNAL_PAYLOAD_LIMITS) })
    }
    const joinedTurn = previous?.turn ?? turn
    const snapshot: ToolSnapshot = {
      body,
      ...(joinedTurn === undefined ? {} : { turn: joinedTurn })
    }
    this.remember(update.toolCallId, snapshot)
    const join = joinedTurn === undefined ? {} : { join: { turn: joinedTurn } }
    const events: ProviderTimelineEvent[] = [
      {
        type: state === 'running' ? (previous ? 'item.update' : 'item.open') : 'item.close',
        item: `tool:${update.toolCallId}`,
        body,
        ...join
      }
    ]
    for (const content of update.content ?? []) {
      if (content.type === 'diff') {
        events.push({
          type: 'item.update',
          item: `diff:${JSON.stringify([update.toolCallId, content.path])}`,
          body: {
            kind: 'diff',
            path: content.path,
            patch: boundPayload(
              boundedDiffPatch(content.path, content.oldText ?? '', content.newText),
              DEFAULT_JOURNAL_PAYLOAD_LIMITS
            )
          },
          ...join
        })
      } else if (content.type === 'terminal' || content.content.type !== 'text') {
        events.push({
          type: 'provider.frame',
          frameKind: `tool-content:${content.type}`,
          payload: content,
          ...join
        })
      }
    }
    return events
  }

  end(turn: string): void {
    for (const [key, snapshot] of this.tools) {
      if (snapshot.turn === turn) {
        this.bytes -= this.size(key, snapshot)
        this.tools.delete(key)
      }
    }
  }

  private size(key: string, snapshot: ToolSnapshot): number {
    return Buffer.byteLength(key) + Buffer.byteLength(JSON.stringify(snapshot))
  }

  private remember(key: string, snapshot: ToolSnapshot): void {
    const previous = this.tools.get(key)
    const bytes = this.bytes - (previous ? this.size(key, previous) : 0) + this.size(key, snapshot)
    if ((!previous && this.tools.size >= 128) || bytes > 1024 * 1024) {
      throw new Error('ACP tool snapshot budget exceeded')
    }
    this.tools.set(key, snapshot)
    this.bytes = bytes
  }
}
