// One grammar event's writes and the memory change they justify.
//
// Planning reads the assembler's memory and the journal and changes neither: it collects the
// event's transition steps and the commits that update memory. Submitting hands the steps to the
// sink as one transition; the commits run only when the sink admits it, so a refused event leaves
// the assembler exactly as it was and re-applying it is the retry.

import type {
  StructuredAgentSessionEventSink,
  StructuredAgentSessionSinkAdmission
} from '../agent-session-wire/structured-agent-session-event-sink'
import type { StructuredAgentSessionTransitionStep } from '../agent-session-wire/structured-agent-session-transition'

/** The sink calls the assembler needs: one-operation transitions, and the journal as it stands. */
export type ProviderTimelineSink = Required<
  Pick<StructuredAgentSessionEventSink, 'tryAppendTransition' | 'journalItems'>
> &
  Pick<StructuredAgentSessionEventSink, 'setActivity'>

const ADMITTED: StructuredAgentSessionSinkAdmission = { accepted: true }

export class ProviderTimelinePlan {
  private readonly steps: StructuredAgentSessionTransitionStep[] = []
  private readonly commits: (() => void)[] = []
  private readonly landings: ((wrote: boolean) => void)[] = []
  private lifecycle = false
  private refusal: StructuredAgentSessionSinkAdmission | null = null

  /** A write; `landed` hears whether it wrote a row once the transition lands. */
  step(
    step: StructuredAgentSessionTransitionStep,
    options: { lifecycle?: boolean; landed?: (wrote: boolean) => void } = {}
  ): void {
    this.steps.push(step)
    this.landings.push(options.landed ?? (() => {}))
    this.lifecycle ||= options.lifecycle === true || step.kind === 'settlement'
  }

  /** A memory change that holds only once the sink takes the writes. */
  commit(change: () => void): void {
    this.commits.push(change)
  }

  /** The event cannot be taken at all (over budget); nothing is submitted. */
  refuse(admission: StructuredAgentSessionSinkAdmission): void {
    this.refusal ??= admission
  }

  get writes(): number {
    return this.steps.length
  }

  submit(sink: ProviderTimelineSink): StructuredAgentSessionSinkAdmission {
    if (this.refusal) {
      return this.refusal
    }
    if (this.steps.length > 0) {
      const landings = this.landings
      const admission = sink.tryAppendTransition({
        steps: this.steps,
        lifecycle: this.lifecycle,
        publish: true,
        landed: (wrote) => wrote.forEach((each, index) => landings[index]?.(each))
      })
      if (!admission.accepted) {
        return admission
      }
    }
    for (const change of this.commits) {
      change()
    }
    return ADMITTED
  }
}

/** The sink as the assembler needs it; null for a sink without transitions or a journal view. */
export function providerTimelineSink(
  sink: StructuredAgentSessionEventSink
): ProviderTimelineSink | null {
  const { tryAppendTransition, journalItems, setActivity } = sink
  if (!tryAppendTransition || !journalItems) {
    return null
  }
  return {
    tryAppendTransition: (transition) => tryAppendTransition.call(sink, transition),
    journalItems: () => journalItems.call(sink),
    ...(setActivity ? { setActivity: (activity) => setActivity.call(sink, activity) } : {})
  }
}
