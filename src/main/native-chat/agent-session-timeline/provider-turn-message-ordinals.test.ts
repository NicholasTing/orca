import { expect, it } from 'vitest'
import {
  ProviderTurnMessageOrdinals,
  MAX_PROVIDER_TURN_ORDINAL_BYTES,
  MAX_PROVIDER_TURN_ORDINAL_ENTRIES
} from './provider-turn-message-ordinals'

it('does not rescan the forgotten window for each new streamed item', () => {
  const ordinals = new ProviderTurnMessageOrdinals()
  for (let index = 0; index < MAX_PROVIDER_TURN_ORDINAL_ENTRIES; index += 1) {
    ordinals.ordinalFor('thread', String(index), 'item')
    ordinals.forgetTurn('thread', String(index))
  }
  // oxlint-disable-next-line typescript/consistent-type-assertions -- SAFETY: this test counts reads of the class's private turn map, whose entries carry `active`.
  const turns = (ordinals as unknown as { turns: Map<string, { active: boolean }> }).turns
  let reads = 0
  for (const turn of turns.values()) {
    let active = turn.active
    Object.defineProperty(turn, 'active', {
      get() {
        reads += 1
        return active
      },
      set(value: boolean) {
        active = value
      }
    })
  }
  for (let index = 0; index < 1000; index += 1) {
    expect(ordinals.ordinalFor('thread', 'live', String(index))).toBe(index)
  }
  expect(reads).toBe(0)
  expect(ordinals.forgottenTurnCount).toBe(MAX_PROVIDER_TURN_ORDINAL_ENTRIES)
})

it('retains ordinal continuity on late reactivation and evicts oldest forgotten turns', () => {
  const ordinals = new ProviderTurnMessageOrdinals()
  expect(ordinals.ordinalFor('t', 'first', 'a')).toBe(0)
  ordinals.forgetTurn('t', 'first')
  expect(ordinals.ordinalFor('t', 'first', 'b')).toBe(1)
  expect(ordinals.forgottenTurnCount).toBe(0)
  ordinals.forgetTurn('t', 'first')
  for (let index = 0; index < MAX_PROVIDER_TURN_ORDINAL_ENTRIES; index += 1) {
    ordinals.ordinalFor('t', String(index), 'a')
    ordinals.forgetTurn('t', String(index))
  }
  expect(ordinals.forgottenTurnCount).toBe(MAX_PROVIDER_TURN_ORDINAL_ENTRIES)
  expect(ordinals.ordinalFor('t', 'first', 'c')).toBe(0)
})

it('keeps byte eviction bounded for active and forgotten turns', () => {
  const ordinals = new ProviderTurnMessageOrdinals()
  for (let index = 0; index < 4000; index += 1) {
    ordinals.ordinalFor('thread', 'live', `${index}-${'x'.repeat(240)}`)
  }
  expect(ordinals.bytes).toBeLessThanOrEqual(MAX_PROVIDER_TURN_ORDINAL_BYTES)
  ordinals.forgetTurn('thread', 'live')
  expect(ordinals.bytes).toBeLessThan(100)
  expect(ordinals.forgottenTurnCount).toBe(1)
})
