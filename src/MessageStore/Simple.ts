import { Message, MessageStoreOperations } from "../types";

import createLogger from "../../utils/logger";

const logger = createLogger("message-store:simple");

/**
 * * Implementation of a message store for CRDT Sync Module.
 * * Utilizes In-Memory mappings to store and retrieve previous messages that have been consumed by CRDT Sync
 * @class SimpleMessageStore.
 */
class SimpleMessageStore implements MessageStoreOperations  {
  messages: Message[] = [];

  constructor() {
    this.messages = [];
  }

  async initialize() {
    logger.info(`Message Store Initialized`);
  }

  async getMessages() {
    return this.messages;
  }

  async add(message: Message) {
    this.messages.push(message);
  }

  async compare(messages: Message[]) {
    let existingMessages = new Map();

    // This could be optimized, but keeping it simple for now. Need to
    // find the latest message that exists for the dataset/row/column
    // for each incoming message, so sort it first

    const stored_messages = await this.getMessages();

    let sortedMessages = [...stored_messages].sort((m1, m2) => {
      if (m1.timestamp < m2.timestamp) {
        return 1;
      } else if (m1.timestamp > m2.timestamp) {
        return -1;
      }
      return 0;
    });

    messages.forEach((msg1) => {
      let existingMsg = sortedMessages.find(
        (msg2) =>
          msg1.dataset === msg2.dataset &&
          msg1.row === msg2.row &&
          msg1.column === msg2.column
      );

      existingMessages.set(msg1, existingMsg);
    });

    return existingMessages;
  }
}

export default SimpleMessageStore;
