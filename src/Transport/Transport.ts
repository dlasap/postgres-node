import Clock from "../Core/Clock";
import { Message, MessageCallback } from "../types";

/**
 *  * Used by the Sync Class to send and/or recieve messages from
 *    * (1) Message Broker
 *    * (2) Peer Nodes
 *  * Usage: Virtual Class - do not use directly. Instead extend this class and implement
 * @class Transport
 * @classdesc Description Generic/Virtual Class for message transport.
 * @description Transport: None
 */

class Transport {
  outgoing_messages: Message[];
  enabled: boolean;
  clock?: Clock;
  recieveCallback: MessageCallback;

  constructor() {
    this.outgoing_messages = [];
    this.enabled = true;
    this.recieveCallback = () => {};
  }

  setCallback(callback: MessageCallback) {
    this.recieveCallback = callback;
  }

  setClock(_clock: Clock) {
    this.clock = _clock;
  }

  enable() {
    this.enabled = true;
  }

  disable() {
    this.enabled = false;
  }

  async initialize() {}

  send(message: Message[]): void {
    if (!this.clock) {
      throw new Error(
        `Transport Clock not defined. invoke tranport.setClock(clockInstance)`
      );
    }

    this.outgoing_messages = [...this.outgoing_messages, ...message];
  }

  sendIndex(message: Message[]): void {
    if (!this.clock) {
      throw new Error(
        `Transport Clock not defined. invoke tranport.setClock(clockInstance)`
      );
    }
    this.outgoing_messages = [...this.outgoing_messages, ...message];
  }
}

export default Transport;
