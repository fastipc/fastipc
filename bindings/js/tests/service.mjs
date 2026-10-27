// A service's owner, clients and servers, as the Python binding's tests/service_peer.py runs them: the tests run them
// here, and tests/peer.mjs in other processes. A client sends `count` requests, ping 1 to ping <count> (the texts
// `ping <i>`), one at a time (call), and checks that each reply is its request's uppercase, PING <i>. The owner's
// service has rings of 4096 bytes, requests and replies of up to 256 bytes, and room for 8 clients and 8 servers.
import assert from 'node:assert/strict';

import { CLIENT, FipcError, SERVER, createService, joinService } from '../index.mjs';

export const TIMEOUT = 20000;
const CAPACITY = 4096;
const MAX_MESSAGE = 256;
const MEMBERS = 8;

/** Request `i`: the text `ping <i>` */
export function request(i) {
  return `ping ${i}`;
}

/** A request's reply: its uppercase (ASCII, as Python's bytes.upper()) */
export function upper(message) {
  return Buffer.from(message).toString('latin1').toUpperCase();
}

/** Whether `error` is the service's end; any other error is thrown again */
function isEnd(error) {
  if (error instanceof FipcError && error.code === 'FIPC_DISCONNECTED') return true;
  throw error;
}

/** Answers `count` requests, each with its uppercase */
export async function answer(server, count) {
  for (let i = 0; i < count; i++) await server.send(upper(await server.receive(TIMEOUT)));
}

/** Answers requests until the service ends; how many it answered */
export async function answerUntilTheEnd(server) {
  for (let answered = 0; ; answered++) {
    let message;
    try {
      message = await server.receive(TIMEOUT);
      await server.send(upper(message));
    } catch (error) {
      if (isEnd(error)) return answered; // the end, before a request or between a request and its reply
    }
  }
}

/** Calls `count` times, ping 1 to ping <count>, and checks each reply */
export async function call(client, count) {
  for (let i = 1; i <= count; i++) {
    const reply = await client.call(request(i), TIMEOUT);
    assert.equal(reply.toString(), upper(request(i)), `the reply to ${request(i)}`);
  }
}

/**
 * Creates the service on `name` and joins it as a server, as the examples' server does, answers `count` requests of
 * each of `clients` clients, and closes its server and the service, which ends it.
 */
export async function serve(name, count, clients = 1) {
  const service = createService(name, CAPACITY, MAX_MESSAGE, MEMBERS, MEMBERS);
  try {
    const server = await joinService(name, SERVER, TIMEOUT);
    try {
      await answer(server, count * clients);
    } finally {
      server.close();
    }
  } finally {
    service.close();
  }
}

/** Joins `name` as a client, calls `count` times and checks each reply, then closes */
export async function joinAndCall(name, count) {
  const client = await joinService(name, CLIENT, TIMEOUT);
  try {
    await call(client, count);
  } finally {
    client.close();
  }
}

/**
 * Creates the service on `name` and joins it as a client, waits for `servers` servers to have joined, calls `count`
 * times (with callSync if `sync`), checks each reply, and closes its client and the service, which ends it.
 */
export async function ownAndCall(name, count, servers = 1, sync = false) {
  const service = createService(name, CAPACITY, MAX_MESSAGE, MEMBERS, MEMBERS);
  try {
    const client = await joinService(name, CLIENT, TIMEOUT);
    try {
      assert.ok(await service.waitForMembers(SERVER, servers, TIMEOUT) >= servers);
      if (!sync) {
        await call(client, count);
      } else {
        for (let i = 1; i <= count; i++) {
          assert.equal(client.callSync(request(i), TIMEOUT).toString(), upper(request(i)));
        }
      }
    } finally {
      client.close();
    }
  } finally {
    service.close();
  }
}

/** Joins `name` as a server and answers requests until the service's end */
export async function joinAndAnswer(name) {
  const server = await joinService(name, SERVER, TIMEOUT);
  try {
    await answerUntilTheEnd(server);
  } finally {
    server.close();
  }
}

/** Joins `name` as a server, takes one request, prints `holding` and waits, never replying, until it is killed */
export async function joinAndHold(name) {
  const server = await joinService(name, SERVER, TIMEOUT);
  await server.receive(TIMEOUT);
  console.log('holding');
  setTimeout(() => {
    console.error('holding server: not killed within 60 s');
    process.exit(1);
  }, 60000);
  setInterval(() => server.maxPiece, 1000); // keeps the connection in reach: a dropped one is closed when collected
}
