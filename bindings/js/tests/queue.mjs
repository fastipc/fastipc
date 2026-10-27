// A work queue's owner, producers and consumers, as the Python binding's tests/queue_peer.py runs them: the tests run
// them here, and tests/peer.mjs in other processes. A producer sends job 1 to job <count>, the texts `job <i>`, or
// with a `size`, `job <i>` padded to size(i) bytes. The owner's queue has rings of `capacity` bytes (4096 by default:
// 64 messages of these lengths before a producer waits for room) and room for 8 producers and 8 consumers.
import assert from 'node:assert/strict';

import { CONSUMER, PRODUCER, createQueue, joinQueue } from '../index.mjs';

export const TIMEOUT = 20000;
const MEMBERS = 8;

/** Job `i`: the text `job <i>`, padded with a pattern to `size` bytes if given */
export function job(i, size) {
  const text = Buffer.from(`job ${i}`);
  if (size === undefined) return text;
  const bytes = Buffer.alloc(size);
  text.copy(bytes);
  bytes[text.length] = 0x20;
  for (let at = text.length + 1; at < size; at++) bytes[at] = (at * 31 + i) & 0xff;
  return bytes;
}

/**
 * The jobs' sizes by name, for the peers: `long` jobs are of 16 to 16015 bytes, longer and shorter in turn, for a
 * queue of 64 KiB rings (16 KiB pieces), so consumers that race for them take the next one after another took theirs
 */
export const SIZES = { long: (i) => 16 + ((i * 7919) % 16000) };

/** The number of a job `job()` made, checking its bytes: size(i) of them if `size` is given */
export function jobNumber(data, size) {
  const match = /^job (\d+)/.exec(data.subarray(0, 16).toString('latin1'));
  assert.ok(match, `not a job: ${data.subarray(0, 16).toString('latin1')}`);
  const i = Number(match[1]);
  if (size !== undefined) {
    assert.equal(data.length, size(i), `job ${i}'s length`);
    assert.ok(data.equals(job(i, size(i))), `job ${i}'s bytes`);
  } else {
    assert.equal(data.toString(), `job ${i}`);
  }
  return i;
}

/** Receives until the queue's end (FIPC_DISCONNECTED): the messages received */
export async function receiveAll(consumer) {
  const received = [];
  for (;;) {
    try {
      received.push(await consumer.receive(TIMEOUT));
    } catch (error) {
      if (error.code === 'FIPC_DISCONNECTED') return received;
      throw error;
    }
  }
}

/**
 * That `received` (job numbers) is the jobs of `producers` producers, each sending job 1 to job <count>, in any order:
 * each job `producers` times.
 */
export function checkJobs(received, producers, count) {
  const times = new Array(count + 1).fill(0);
  for (const i of received) {
    assert.ok(i >= 1 && i <= count, `received job ${i}, of jobs 1 to ${count}`);
    times[i] += 1;
    assert.ok(times[i] <= producers, `received job ${i} more than ${producers} times`);
  }
  assert.equal(received.length, producers * count, `received ${received.length} of ${producers * count} jobs`);
}

/** That the consumers' `lists` (job numbers) split one producer's job 1 to job <count>: each once, in any order */
export function checkSplit(lists, count) {
  const seen = new Set();
  lists.forEach((list, c) => {
    for (const i of list) {
      assert.ok(!seen.has(i), `job ${i} went to two consumers (the second: consumer ${c})`);
      seen.add(i);
    }
  });
  assert.equal(seen.size, count, `received ${seen.size} of ${count} jobs`);
}

/**
 * Creates the queue on `name` and joins it as a producer, as the examples' producer does, sends the jobs (more than
 * its ring holds wait for a consumer to take some), waits for `consumers` consumers to have joined, and closes its
 * producer and the queue, which ends it: its consumers receive what is left, then FIPC_DISCONNECTED.
 */
export async function produce(name, count, consumers, { capacity = 4096, size } = {}) {
  const queue = createQueue(name, capacity, MEMBERS, MEMBERS);
  try {
    const producer = await joinQueue(name, PRODUCER, TIMEOUT);
    try {
      for (let i = 1; i <= count; i++) await producer.send(job(i, size?.(i)), TIMEOUT);
      assert.ok(await queue.waitForMembers(CONSUMER, consumers, TIMEOUT) >= consumers);
    } finally {
      producer.close();
    }
  } finally {
    queue.close();
  }
}

/** Joins `name` as a consumer and receives until the queue's end: the job numbers, checked (and their lengths) */
export async function consume(name, size) {
  const consumer = await joinQueue(name, CONSUMER, TIMEOUT);
  try {
    return (await receiveAll(consumer)).map((data) => jobNumber(data, size));
  } finally {
    consumer.close();
  }
}

/**
 * Creates the queue on `name` and joins it as a consumer, receives `count` jobs from each of `producers` producers,
 * checks that each job of each producer came once, and closes its consumer and the queue, which ends it.
 */
export async function ownAndConsume(name, count, producers) {
  const queue = createQueue(name, 4096, MEMBERS, MEMBERS);
  const received = [];
  try {
    const consumer = await joinQueue(name, CONSUMER, TIMEOUT);
    try {
      while (received.length < producers * count) received.push(jobNumber(await consumer.receive(TIMEOUT)));
    } finally {
      consumer.close();
    }
  } finally {
    queue.close();
  }
  checkJobs(received, producers, count);
}

/** Joins `name` as a producer, sends the jobs and closes; the queue goes on until its owner ends it */
export async function joinAndProduce(name, count) {
  const producer = await joinQueue(name, PRODUCER, TIMEOUT);
  try {
    for (let i = 1; i <= count; i++) await producer.send(job(i), TIMEOUT);
  } finally {
    producer.close();
  }
}
