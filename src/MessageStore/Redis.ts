import SimpleMessageStore from "./Simple";
import { Message, RedisConfig, MessageStoreOperations } from "../types";
import bluebird from "bluebird";
import Redis from "ioredis";
import createLogger from "../../utils/logger";

const logger = createLogger("message-store:redis");
interface MessageVector {
  dataset: string;
  row: string;
  column: string;
}

/**
 * * Implementation of a message store for CRDT Sync Module.
 * * Utilizes Redis Sets to store and retrieve previous messages that have been consumed by CRDT Sync
 * @class RedisMessageStore.
 * @param {object} config - Redis Configuration
 * @param {number} message_expiry_seconds - Seconds to wait before expiring the message. Is refreshed when message is accessed.
 */
class RedisMessageStore
  extends SimpleMessageStore
  implements MessageStoreOperations
{
  redis_config: RedisConfig;
  redis_client?: any; // Broken Type definition in IORedis
  message_expiry_seconds: number;

  constructor(config: RedisConfig, message_expiry_seconds: number = 20600) {
    super();
    this.redis_config = config;
    this.message_expiry_seconds = message_expiry_seconds;

    //@ts-ignore
    this.redis_client = new Redis(config as Redis.RedisOptions);
    this.redis_client.on("connect", () => {
      logger.info(`Redis Connected to ${config.host}:${config.port}`);
    });
  }

  async initialize() {
    await super.initialize();

    logger.info(`Message Store Initialized`);
  }

  async add(message: Message) {
    try {
      const { timestamp = 0, dataset, column, row } = message;
      const main_set = `messages_crdt`;
      const payload = JSON.stringify(message);
      const key = `message_crdt-${dataset}:${row}:${column}:${timestamp}`;
      const key_vector = `message_crdt:${dataset}:${row}:${column}`;
      await this.redis_client?.set(key, payload);
      await this.redis_client?.sadd(main_set, key);
      await this.redis_client?.sadd(key_vector, key);

      await this.redis_client?.expire(key, this.message_expiry_seconds);
      await this.redis_client?.expire(key_vector, this.message_expiry_seconds);
      await this.redis_client?.expire(main_set, this.message_expiry_seconds);
    } catch (e) {
      logger.error(`[Error]: ${e}`);
    }
  }

  async getMessages() {
    const key_vector = `messages_crdt`;
    const keys = await this.redis_client?.smembers(key_vector);
    const messages = await bluebird.map(keys, async (key) => {
      const payload = await this.redis_client?.get(key);
      await this.redis_client?.expire(key, this.message_expiry_seconds);
      return JSON.parse(payload);
    });
    await this.redis_client?.expire(key_vector, this.message_expiry_seconds);
    return messages;
  }

  async getMessagesByVector({ dataset, row, column }: MessageVector) {
    const key_vector = `message_crdt:${dataset}:${row}:${column}`;
    const keys = await this.redis_client?.smembers(key_vector);
    const messages = await bluebird.mapSeries(keys, async (key) => {
      const payload = await this.redis_client?.get(key);
      await this.redis_client?.expire(key, this.message_expiry_seconds);
      return JSON.parse(payload);
    });

    await this.redis_client?.expire(key_vector, this.message_expiry_seconds);
    return messages;
  }

  async compare(messages: Message[]) {
    // logger.info(`[MESSAGES]: ${JSON.stringify(messages, null, 2)}`)
    const existingMessages = new Map();
    await bluebird.each(messages, async (msg1) => {
      const stored_messages = await this.getMessagesByVector(
        msg1 as MessageVector
      );

      const sorted_messages = stored_messages.sort((m1, m2) => {
        try {
          if (m1?.timestamp < m2?.timestamp) {
            return 1;
          } else if (m1.timestamp > m2.timestamp) {
            return -1;
          }
        } catch (e) {
          logger.error(`[Timestamp][Error]: ${e}`);
          logger.info(`[Timestamp][m1]: ${m1}`);
          logger.info(`[Timestamp][m2]: ${m2}`);
          return 0;
        }
        return 0;
      });

      let existingMsg = sorted_messages.find((msg2) => {
        try {
          return (
            msg1?.dataset === msg2?.dataset &&
            msg1?.row === msg2?.row &&
            msg1?.column === msg2?.column
          );
        } catch (e) {
          logger.error(`[existingMsg][Error]: ${e}`);
          logger.info(`[msg1][Error]: ${msg1}`);
          logger.info(`[msg2][Error]: ${msg2}`);
          return false;
        }
      });

      if (msg1 && existingMsg) existingMessages.set(msg1, existingMsg);
    });
    return existingMessages;
  }
}

export default RedisMessageStore;
