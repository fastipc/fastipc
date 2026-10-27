// A broadcast's publisher and subscriber, as the Python binding's tests/broadcast_peer.py runs them: the tests
// run them here, and tests/peer.mjs in other processes. Message i, from 0 to count - 1, is the text `message <i>`.
import assert from 'node:assert/strict';

import { SUBSCRIBER_GATED, SUBSCRIBER_LOSSY, createBroadcast, joinBroadcast } from '../index.mjs';

export const TIMEOUT = 20000;

export function message(i) {
  return `message ${i}`;
}

/**
 * Creates a broadcast of 4096 bytes on `name` (64 messages of these lengths before a gated subscriber holds the
 * publisher back) and up to 8 subscribers, waits for `subscribers` of them, sends `count` messages and closes, which
 * ends the broadcast.
 */
export async function publish(name, count, subscribers) {
  const publisher = createBroadcast(name, 4096, 8, TIMEOUT);
  try {
    assert.ok(await publisher.waitForMembers(subscribers, TIMEOUT) >= subscribers);
    for (let i = 0; i < count; i++) await publisher.send(message(i), TIMEOUT);
  } finally {
    publisher.close();
  }
}

/**
 * Joins `name` as a gated or lossy subscriber and receives until FIPC_DISCONNECTED. A gated one checks that it
 * received every message in order; a lossy one that each message it received is the one after the last, or that many
 * after it as an FIPC_OVERRUN between them reported lost, and that received and lost add up to `count`.
 */
export async function subscribe(name, gated, count) {
  const role = gated ? 'gated' : 'lossy';
  const subscriber = await joinBroadcast(name, gated ? SUBSCRIBER_GATED : SUBSCRIBER_LOSSY, TIMEOUT);
  let expected = 0; // the index of the next message
  let received = 0;
  let lost = 0;
  try {
    for (;;) {
      let data;
      try {
        data = await subscriber.receive(TIMEOUT);
      } catch (error) {
        if (error.code === 'FIPC_DISCONNECTED') break;
        if (error.code !== 'FIPC_OVERRUN' || gated) throw error;
        assert.ok(error.lost >= 1, `${role}: ${error.lost} lost`);
        lost += error.lost;
        expected += error.lost;
        continue;
      }
      assert.equal(data.toString(), message(expected), `${role}: the message after ${received} received`);
      received += 1;
      expected += 1;
    }
  } finally {
    subscriber.close();
  }
  assert.equal(expected, count, `${role}: received ${received} and lost ${lost} of ${count} messages`);
  if (gated) assert.equal(received, count, 'a gated subscriber lost messages');
}
