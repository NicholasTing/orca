// One admission budget for everything the assembler holds open: running items, pending requests
// and open text streams. A provider that opens more than this at once is refused rather than
// silently forgotten: the refusal is `failed`, which ends the session the way a failed journal
// write does, and the journal-derived dead-generation settlement closes whatever it left open.

import type { StructuredAgentSessionSinkAdmission } from '../agent-session-wire/structured-agent-session-event-sink'

export const MAX_PROVIDER_TIMELINE_OPEN_ENTRIES = 128
export const MAX_PROVIDER_TIMELINE_OPEN_BYTES = 1024 * 1024

export const PROVIDER_TIMELINE_OVER_BUDGET: StructuredAgentSessionSinkAdmission = {
  accepted: false,
  reason: 'failed'
}

export function providerTimelineEntryBytes(...values: unknown[]): number {
  return values.reduce<number>(
    (total, value) => total + Buffer.byteLength(JSON.stringify(value) ?? '', 'utf8'),
    0
  )
}

export class ProviderTimelineBudget {
  private readonly held = new Map<string, number>()
  private bytes = 0

  constructor(
    private readonly limits = {
      entries: MAX_PROVIDER_TIMELINE_OPEN_ENTRIES,
      bytes: MAX_PROVIDER_TIMELINE_OPEN_BYTES
    }
  ) {}

  /** Whether `id` may hold `bytes`; an entry already held is re-measured, not added. */
  admits(id: string, bytes: number): boolean {
    const current = this.held.get(id)
    const entries = this.held.size + (current === undefined ? 1 : 0)
    return (
      entries <= this.limits.entries && this.bytes - (current ?? 0) + bytes <= this.limits.bytes
    )
  }

  hold(id: string, bytes: number): void {
    this.bytes += bytes - (this.held.get(id) ?? 0)
    this.held.set(id, bytes)
  }

  release(id: string): void {
    this.bytes -= this.held.get(id) ?? 0
    this.held.delete(id)
  }

  clear(): void {
    this.held.clear()
    this.bytes = 0
  }

  get size(): number {
    return this.held.size
  }
}
