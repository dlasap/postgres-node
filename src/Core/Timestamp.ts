/*
  Filename: timestamp.ts
  Description:  Typescript port from timestamp.js by James Long
*/

import murmurhash from '../../utils/murmurhash';
import Clock, { ClockPrototype } from './Clock';
import { TimestampState, TimestampInitOptions } from './../types';

const { HLC_MAX_CLOCK_DRIFT = '60000' } = process.env;

const config = {
  // Maximum physical clock drift allowed, in ms
  maxDrift: parseInt(HLC_MAX_CLOCK_DRIFT),
};
class ClockDriftError extends Error {
  type: string;
  constructor(...args: any[]) {
    super();
    this.type = 'ClockDriftError';
    this.message = ['maximum clock drift exceeded'].concat(args).join(' ');
  }
}

class OverflowError extends Error {
  type: string;
  constructor() {
    super();
    this.type = 'OverflowError';
    this.message = 'timestamp counter overflow';
  }
}

class DuplicateNodeError extends Error {
  type: string;
  constructor(node: any) {
    super();
    this.type = 'DuplicateNodeError';
    this.message = 'duplicate node identifier ' + node;
  }
}

class Timestamp {
  _state: TimestampState;

  constructor(millis: number, counter: number, node: any) {
    this._state = {
      millis: millis,
      counter: counter,
      node: node,
    };
  }

  valueOf() {
    return this.toString();
  }

  toString() {
    return [
      new Date(this.millis()).toISOString(),
      ('0000' + this.counter().toString(16).toUpperCase()).slice(-4),
      ('0000000000000000' + this.node()).slice(-16),
    ].join('-');
  }

  millis() {
    return this._state.millis;
  }

  counter() {
    return this._state.counter;
  }

  node() {
    return this._state.node;
  }

  hash() {
    return murmurhash.v3(this.toString());
  }

  init(options: TimestampInitOptions = {}) {
    if (options?.maxDrift) {
      config.maxDrift = options?.maxDrift;
    }
  }

  /**
   * Timestamp send. Generates a unique, monotonic timestamp suitable
   * for transmission to another system in string format
   */
  static send(clock: ClockPrototype) {
    // Retrieve the local wall time
    const phys = Date.now();

    // Unpack the clock.timestamp logical time and counter
    const lOld = clock.timestamp.millis();
    const cOld = clock.timestamp.counter();

    // Calculate the next logical time and counter
    // * ensure that the logical time never goes backward
    // * increment the counter if phys time does not advance
    const lNew = Math.max(lOld, phys);
    const cNew = lOld === lNew ? cOld + 1 : 0;

    // Check the result for drift and counter overflow
    if (lNew - phys > config.maxDrift) {
      throw new ClockDriftError(lNew, phys, config.maxDrift);
    }
    if (cNew > 65535) {
      throw new OverflowError();
    }

    // Repack the logical time/counter
    clock.timestamp.setMillis(lNew);
    clock.timestamp.setCounter(cNew);

    return new Timestamp(
      clock.timestamp.millis(),
      clock.timestamp.counter(),
      clock.timestamp.node()
    );
  }

  // Timestamp receive. Parses and merges a timestamp from a remote
  // system with the local timeglobal uniqueness and monotonicity are
  // preserved
  static recv(clock: ClockPrototype, msg: any) {
    const phys = Date.now();

    // Unpack the message wall time/counter
    const lMsg = msg.millis();
    const cMsg = msg.counter();

    // Assert the node id and remote clock drift
    if (msg.node() === clock.timestamp.node()) {
      // throw new DuplicateNodeError(clock.timestamp.node());
    }
    if (lMsg - phys > config.maxDrift) {
      throw new ClockDriftError();
    }

    // Unpack the clock.timestamp logical time and counter
    const lOld = clock.timestamp.millis();
    const cOld = clock.timestamp.counter();

    // Calculate the next logical time and counter.
    // Ensure that the logical time never goes backward;
    // * if all logical clocks are equal, increment the max counter,
    // * if max = old > message, increment local counter,
    // * if max = messsage > old, increment message counter,
    // * otherwise, clocks are monotonic, reset counter
    const lNew = Math.max(Math.max(lOld, phys), lMsg);
    const cNew =
      lNew === lOld && lNew === lMsg
        ? Math.max(cOld, cMsg) + 1
        : lNew === lOld
        ? cOld + 1
        : lNew === lMsg
        ? cMsg + 1
        : 0;

    // Check the result for drift and counter overflow
    if (lNew - phys > config.maxDrift) {
      throw new ClockDriftError();
    }
    if (cNew > 65535) {
      throw new OverflowError();
    }

    // Repack the logical time/counter
    clock.timestamp.setMillis(lNew);
    clock.timestamp.setCounter(cNew);

    return new Timestamp(
      clock.timestamp.millis(),
      clock.timestamp.counter(),
      clock.timestamp.node()
    );
  }

  /**
   * Converts a fixed-length string timestamp to the structured value
   */
  static parse(timestamp: Timestamp | string) {
    if (typeof timestamp === 'string') {
      const parts = timestamp.split('-');
      if (parts && parts.length === 5) {
        const millis = Date.parse(parts.slice(0, 3).join('-')).valueOf();
        const counter = parseInt(parts[3], 16);
        const node = parts[4];
        if (!isNaN(millis) && !isNaN(counter))
          return new Timestamp(millis, counter, node);
      }
    }
    return null;
  }

  static since(isoString: string) {
    return isoString + '-0000-0000000000000000';
  }
}

class MutableTimestamp extends Timestamp {
  setMillis(n: number) {
    this._state.millis = n;
  }

  setCounter(n: number) {
    this._state.counter = n;
  }

  setNode(n: number) {
    this._state.node = n;
  }

  static from(timestamp: Timestamp) {
    return new MutableTimestamp(
      timestamp.millis(),
      timestamp.counter(),
      timestamp.node()
    );
  }
}

export { Timestamp, MutableTimestamp };
