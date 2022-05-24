import { Interpreter, State } from "xstate";

import { Interpret, IContext, IMachineEvents } from "../../machines/node";

import { MutableTimestamp, Timestamp } from "./Timestamp";
import * as merkle from "../../utils/merkle";
import Clock from "./Clock";

import bluebird from "bluebird";
import { Message } from "../types";

import Transport from "../Transport";
import MessageStore from "../MessageStore";

import createLogger from "../../utils/logger";

const logger = createLogger("sync");

/**
 * * Implementation of Conflict-free Replicated Data Type (CRDT).
 * @class Sync
 * @param {object} transport - typeof Transport. Transport mechanism for sending/recieving CRDT Messages.
 * @param {object} message_store - typeof MessageStore. Storage mechanism for evaluation of messages.
 */
class Sync {
  _timestamp: MutableTimestamp = new MutableTimestamp(
    0,
    0,
    Clock.makeClientId()
  );
  clock: Clock = new Clock(this._timestamp, {});
  // messages: Message[] = [];
  transport: Transport;

  message_store: MessageStore;

  server_state: State<IContext, IMachineEvents>;
  service: Interpreter<IContext, any, IMachineEvents>;

  // apply?: (message: Message) => Promise<void>

  constructor(transport: Transport, message_store: MessageStore) {
    this.message_store = message_store;
    this.transport = transport;
    this.transport.setClock(this.clock);
    this.transport.setCallback(this.receiveMessages.bind(this));

    this.service = Interpret({});
    this.server_state = this.service.initialState;

    this.service.onTransition(this.onServiceTransition.bind(this));

    this.service.start();
  }

  getState() {
    return this.server_state;
  }

  onServiceTransition(state: State<IContext, IMachineEvents>) {
    this.server_state = state;
  }

  async initialize() {
    this.server_state = this.service.send("INITIALIZING");
    await this.transport.initialize();
    await this.message_store.initialize();
    this.server_state = this.service.send("INITIALIZED");
  }

  async applyMessages(messages: Message[]) {
    const existingMessages = await this.message_store.compare(messages);
    const clock = this.clock.getClock();

    await bluebird.mapSeries(messages, async (msg) => {
      const existingMsg = existingMessages.get(msg);

      // logger.info(`Applying message: id:${msg.id}`);

      if (!existingMsg || existingMsg.timestamp < msg.timestamp) {
        //@ts-ignore
        await this.apply(msg);
      }

      if (!existingMsg || existingMsg.timestamp !== msg.timestamp) {
        //@ts-ignore
        clock?.merkle = await merkle.insert(
          clock?.merkle,
          Timestamp.parse(msg.timestamp) as Timestamp
        );
        await this.message_store.add(msg);
      }
      // console.log("@msg::: ", msg);
    });
  }

  async apply(msg: Message): Promise<void> {
    throw new Error(`Message Applicator not implemented.`);
  }

  async sendMessages(messages: Message[]) {
    await this.applyMessages(messages);

    //@ts-ignore
    this.transport.send(messages, true);
  }

  async sendIndex(message: Message) {
    this.transport.sendIndex([message]);
  }

  async receiveMessages(messages: Message[]) {
    await bluebird.each(messages, (msg) => {
      const clock = this.clock.getClock();
      Timestamp.recv(clock, Timestamp.parse(msg.timestamp));
    });

    this.service.send("MESSAGE");
    await this.applyMessages(messages);
  }
}

export default Sync;
