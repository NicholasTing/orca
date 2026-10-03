import { afterEach, describe, expect, it } from 'vitest'
import { parseAgentJournalItemKey } from '../../../shared/agent-session-journal-item-key'
import {
  closeProviderTimelineRigs,
  openProviderTimelineRig,
  pendingApproval,
  providerItemId,
  providerTurnItemId,
  runningTool
} from './provider-timeline-assembler-test-support'

afterEach(closeProviderTimelineRigs)

const answered = {
  ...pendingApproval,
  resolution: {
    state: 'resolved' as const,
    selectedOptionId: 'allow',
    resolvedBy: 'phone',
    resolvedAt: 1_500
  }
}

describe('a replay never changes what the journal already holds', () => {
  it('keeps an answered prompt answered when a restarted host replays its open and its turn', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 't1', at: 1_000 })
    rig.assembler.apply({ type: 'request.open', request: 'p1', body: pendingApproval })
    const itemId = providerItemId('request', 'p1')
    const identity = parseAgentJournalItemKey(itemId)
    if (!identity) {
      throw new Error('request key did not parse')
    }
    // The answer path's compare-and-set lands first, with no word to the assembler.
    await rig.journal.appendItem(identity, answered, {
      fence: 1,
      turnScope: { kind: 'turn', turnItemId: providerTurnItemId('t1') }
    })
    rig.assembler.apply({ type: 'turn.end', at: 2_000, state: 'completed', outcome: 'success' })
    expect((await rig.row(itemId))?.body).toMatchObject({ resolution: { state: 'resolved' } })

    const restarted = rig.restart({ generation: 'gen-2' })
    expect(restarted.apply({ type: 'turn.open', turn: 't1', at: 3_000 }).dropped).toBe(
      'turn-replayed'
    )
    expect(
      restarted.apply({ type: 'request.open', request: 'p1', body: pendingApproval }).dropped
    ).toBe('request-replayed')
    restarted.apply({ type: 'turn.end', at: 4_000, state: 'completed' })
    expect((await rig.row(itemId))?.body).toMatchObject({
      resolution: { state: 'resolved', selectedOptionId: 'allow' }
    })
  })

  it('does not let a replayed open of a settled turn supersede the live one', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 'old', at: 1_000 })
    rig.assembler.apply({ type: 'turn.end', at: 2_000, state: 'completed' })
    await rig.rows()
    const restarted = rig.restart({ generation: 'gen-2' })
    restarted.apply({ type: 'turn.open', turn: 'live', at: 3_000 })
    restarted.apply({ type: 'item.open', item: 'live-tool', body: runningTool('read') })
    const live = restarted.openTurnId

    expect(restarted.apply({ type: 'turn.open', turn: 'old', at: 1_000 }).dropped).toBe(
      'turn-replayed'
    )
    expect(restarted.openTurnId).toBe(live)
    expect(await rig.turn('live')).toMatchObject({ state: 'running' })
    expect((await rig.row(providerItemId('item', 'live-tool')))?.body).toMatchObject({
      state: 'running'
    })
  })

  it('settles a turn the journal holds running even when the assembler that opened it is gone', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 't1', at: 1_000 })
    rig.assembler.apply({ type: 'item.open', item: 'tool', body: runningTool('read') })
    await rig.rows()
    const restarted = rig.restart({ generation: 'gen-2' })
    restarted.apply({ type: 'turn.end', turn: 't1', at: 2_000, state: 'completed' })
    expect(await rig.turn('t1')).toMatchObject({ state: 'completed', completedAt: 2_000 })
    expect((await rig.row(providerItemId('item', 'tool')))?.body).toMatchObject({ state: 'failed' })
  })

  it('takes its open turn back from the journal when a planned open found the row already there', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 'live', at: 1_000 })
    await rig.rows()
    const live = rig.assembler.openTurnId
    // The plan reads no journal yet (as before it binds); the write still asks it when it runs.
    let lagging = true
    const stale = rig.restart({
      sink: { ...rig.sink, journalItems: () => (lagging ? null : rig.sink.journalItems()) },
      generation: 'gen-2'
    })
    expect(stale.apply({ type: 'turn.open', turn: 'live', at: 1_000 }).dropped).toBeUndefined()
    lagging = false
    await rig.rows()
    await Promise.resolve()
    expect(stale.openTurnId).toBe(live)
    expect(await rig.turn('live')).toMatchObject({ state: 'running', startedAt: 1_000 })
  })

  it('never resurrects a settled tool from a running snapshot', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 't1', at: 1_000 })
    rig.assembler.apply({
      type: 'item.close',
      item: 'tool',
      body: { ...runningTool('read'), state: 'completed' }
    })
    expect(
      rig.assembler.apply({ type: 'item.update', item: 'tool', body: runningTool('read') }).dropped
    ).toBe('item-settled')
    rig.assembler.apply({
      type: 'session.ended',
      verdict: { state: 'interrupted', completedAt: 2_000 }
    })
    expect((await rig.row(providerItemId('item', 'tool')))?.body).toMatchObject({
      state: 'completed'
    })
  })

  it('does not reopen a settled tool a restarted host replays', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 't1', at: 1_000 })
    rig.assembler.apply({ type: 'item.open', item: 'tool', body: runningTool('read') })
    rig.assembler.apply({
      type: 'item.close',
      item: 'tool',
      body: { ...runningTool('read'), state: 'completed' }
    })
    await rig.rows()
    const restarted = rig.restart({ generation: 'gen-2' })
    expect(
      restarted.apply({ type: 'item.open', item: 'tool', body: runningTool('read') }).dropped
    ).toBe('item-replayed')
    expect(
      restarted.apply({ type: 'item.update', item: 'tool', body: runningTool('read') }).dropped
    ).toBe('item-settled')
    expect((await rig.row(providerItemId('item', 'tool')))?.body).toMatchObject({
      state: 'completed'
    })
  })
})

describe('requests', () => {
  it('opens a request reused after it settled as a new one, and withdraws each on its own', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 't1', at: 1_000 })
    rig.assembler.apply({ type: 'request.open', request: 'p1', body: pendingApproval })
    rig.assembler.apply({ type: 'request.withdrawn', request: 'p1' })
    await rig.rows()
    rig.assembler.apply({ type: 'request.open', request: 'p1', body: pendingApproval })
    rig.assembler.apply({ type: 'request.withdrawn', request: 'p1' })

    const prompts = (await rig.rows()).filter((row) => row.body.kind === 'approval')
    expect(prompts.map((row) => row.itemId)).toEqual([
      providerItemId('request', 'p1'),
      providerItemId('request', 'p1', { incarnation: 2 })
    ])
    expect(prompts.map((row) => row.body)).toEqual([
      expect.objectContaining({ resolution: expect.objectContaining({ state: 'cancelled' }) }),
      expect.objectContaining({ resolution: expect.objectContaining({ state: 'cancelled' }) })
    ])
  })

  it('opens a new request when the pending one under its key was answered', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 't1', at: 1_000 })
    rig.assembler.apply({ type: 'request.open', request: 'p1', body: pendingApproval })
    const identity = parseAgentJournalItemKey(providerItemId('request', 'p1'))
    if (!identity) {
      throw new Error('request key did not parse')
    }
    await rig.journal.appendItem(identity, answered, {
      fence: 1,
      turnScope: { kind: 'turn', turnItemId: providerTurnItemId('t1') }
    })
    expect(
      rig.assembler.apply({ type: 'request.open', request: 'p1', body: pendingApproval }).dropped
    ).toBeUndefined()
    // The answered prompt keeps its answer; withdrawing reaches only the new one.
    rig.assembler.apply({ type: 'request.withdrawn', request: 'p1' })
    expect((await rig.row(providerItemId('request', 'p1')))?.body).toMatchObject({
      resolution: { state: 'resolved' }
    })
    expect(
      (await rig.row(providerItemId('request', 'p1', { incarnation: 2 })))?.body
    ).toMatchObject({
      resolution: { state: 'cancelled' }
    })
  })

  it('leaves an answered request answered when its withdrawal lands after the answer', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 't1', at: 1_000 })
    rig.assembler.apply({ type: 'request.open', request: 'p1', body: pendingApproval })
    const identity = parseAgentJournalItemKey(providerItemId('request', 'p1'))
    if (!identity) {
      throw new Error('request key did not parse')
    }
    // Memory still holds it pending; only the journal knows a client answered.
    await rig.journal.appendItem(identity, answered, {
      fence: 1,
      turnScope: { kind: 'turn', turnItemId: providerTurnItemId('t1') }
    })
    rig.assembler.apply({ type: 'request.withdrawn', request: 'p1' })
    expect((await rig.row(providerItemId('request', 'p1')))?.body).toMatchObject({
      resolution: { state: 'resolved', selectedOptionId: 'allow' }
    })
  })

  it('never overwrites a request row it could not see when it planned the open', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 't1', at: 1_000 })
    rig.assembler.apply({ type: 'request.open', request: 'p1', body: pendingApproval })
    const identity = parseAgentJournalItemKey(providerItemId('request', 'p1'))
    if (!identity) {
      throw new Error('request key did not parse')
    }
    await rig.journal.appendItem(identity, answered, {
      fence: 1,
      turnScope: { kind: 'turn', turnItemId: providerTurnItemId('t1') }
    })
    const blind = rig.restart({
      sink: { ...rig.sink, journalItems: () => null },
      generation: 'gen-2'
    })
    blind.apply({ type: 'request.open', request: 'p1', body: pendingApproval })
    expect((await rig.row(providerItemId('request', 'p1')))?.body).toMatchObject({
      resolution: { state: 'resolved', selectedOptionId: 'allow' }
    })
  })
})

describe('a new provider session is a new id space', () => {
  it('gives a reused provider turn id its own row after a reset', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: '1', at: 1_000 })
    rig.assembler.apply({ type: 'turn.end', at: 2_000, state: 'completed' })
    rig.assembler.apply({
      type: 'session.ended',
      verdict: { state: 'interrupted', completedAt: 2_000 }
    })
    rig.assembler.apply({ type: 'session.reset', namespace: 'provider-session-2' })
    rig.assembler.apply({ type: 'turn.open', turn: '1', at: 3_000 })

    expect((await rig.turns()).map((turn) => turn.state)).toEqual(['completed', 'running'])
    expect(await rig.turn('1', 'provider-session-2')).toMatchObject({ startedAt: 3_000 })
  })

  it('does not overwrite an earlier session’s item when the provider reuses its id', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({
      type: 'item.close',
      item: 'tool-1',
      body: { ...runningTool('first'), state: 'completed' }
    })
    rig.assembler.apply({
      type: 'session.ended',
      verdict: { state: 'interrupted', completedAt: 2_000 }
    })
    rig.assembler.apply({ type: 'session.reset', namespace: 'provider-session-2' })
    rig.assembler.apply({
      type: 'item.close',
      item: 'tool-1',
      body: { ...runningTool('second'), state: 'completed' }
    })
    const tools = (await rig.rows()).filter((row) => row.body.kind === 'tool-call')
    expect(tools.map((row) => row.body)).toEqual([
      expect.objectContaining({ name: 'first' }),
      expect.objectContaining({ name: 'second' })
    ])
  })

  it('settles the open work of a session reset before anything ended it, as lost', async () => {
    const rig = await openProviderTimelineRig()
    rig.assembler.apply({ type: 'turn.open', turn: 't1', at: 1_000 })
    rig.assembler.apply({ type: 'item.open', item: 'tool', body: runningTool('read') })
    rig.assembler.apply({
      type: 'text.delta',
      item: { stream: 'reply' },
      channel: 'assistant',
      text: 'Half'
    })
    rig.assembler.apply({ type: 'session.reset', namespace: 'provider-session-2' })
    rig.assembler.apply({ type: 'session.ended', verdict: { state: 'unverifiable' } })

    expect((await rig.row(providerItemId('item', 'tool')))?.body).toMatchObject({ state: 'failed' })
    expect(await rig.turn('t1')).toMatchObject({ state: 'unverifiable' })
    expect((await rig.rows()).filter((row) => row.body.kind === 'message')).toHaveLength(1)
  })
})
