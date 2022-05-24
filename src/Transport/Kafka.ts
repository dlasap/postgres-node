import bluebird from "bluebird";
import createLogger from "../../utils/logger";
import Transport from "./Transport";
import { Message } from "../types";
import os from "os";
import {
  Kafka,
  KafkaConfig,
  Producer,
  Consumer,
  EachBatchPayload,
  // KafkaMessage,
  EachMessagePayload,
} from "kafkajs";
import uuidv4 from "../../utils/uuidv4";

const {
  SYNC_TOPIC = "dna-sync",
  INDEX_TOPIC = "search-sync",
  KAFKA_SYNC_GROUP_ID = "sync-group",
  STORE_NAME = "redis",
} = process.env;

const logger = createLogger("transport:kafka");

const sender_id = uuidv4();
class KafkaTransport extends Transport {
  producer: Producer;
  consumer: Consumer;
  offset: string = "0";
  buffering?: any;
  outgoing_messages_index: Message<any>[] = [];
  stopService?: () => void;
  sending: boolean = false;
  constructor(config: KafkaConfig, group_id = KAFKA_SYNC_GROUP_ID) {
    super();

    const kafka_p = new Kafka(config);
    const kafka_c = new Kafka(config);

    this.producer = kafka_p.producer();
    this.consumer = kafka_c.consumer({
      groupId:
        STORE_NAME === "elastic" ? group_id + "-" + os.hostname() : group_id,
    });

    // setTimeout(this.resendSevice.bind(this), 1);
  }

  async resendSevice() {
    if (this.sending) {
      setTimeout(this.resendSevice.bind(this), 5000);
      return;
    }

    console.log("Checking for skipped messages");
    const batch = [...this.outgoing_messages_index];

    if (batch.length > 0) {
      console.log("RESENDING SKIPPED MESSAGES::", batch.length);

      await this.sendIndex(batch);
      this.outgoing_messages = [];
    }

    setTimeout(this.resendSevice.bind(this), 300);
  }

  async initialize() {
    logger.info(`Transport Initializing: Kafka`);

    await this.producer.connect();
    await this.consumer.connect();

    await this.consumer.subscribe({
      topic: STORE_NAME === "elastic" ? INDEX_TOPIC : SYNC_TOPIC,
    });

    await this.consumer.run({
      autoCommit: true,
      autoCommitInterval: 10000,
      autoCommitThreshold: 1000,
      eachMessage: this.eachMessage.bind(this),
      // eachBatch: this.eachBatch.bind(this),
    });

    logger.info(`Transport Initialized: Kafka`);
  }

  async sendBatch(messages: Message[]) {
    await this.producer.sendBatch({
      topicMessages: [
        {
          topic: SYNC_TOPIC,
          messages: messages.map((m) => {
            return {
              key: m.dataset,
              value: JSON.stringify(m),
            };
          }),
        },
      ],
    });
    // logger.info(`Sent ${messages.length} messages`);
  }
  // getJSONSizeInBytes(obj: Record<string, any>) {
  //   return Buffer.byteLength(JSON.stringify(obj));
  // }

  private async sendIndividually(messages: Message[]) {
    await bluebird.mapSeries(messages, async (msg: Message) => {
      await this.send([msg]);
    });
  }

  async send(messages: Message[], batch: boolean = false) {
    return this.producer
      .send({
        topic: SYNC_TOPIC,
        messages: messages.map((m) => {
          // console.log(
          //   "@@@@@@@@@@@@@@@@@@@@@@@@@@@Message Bytes:: ",
          //   this.getJSONSizeInBytes({ ...m, sender_id })
          // );

          return {
            key: m.dataset,
            value: JSON.stringify({ ...m, sender_id }),
          };
        }),
      })
      .catch((e) => {
        console.error("SEND ERROR", e.message);
        setTimeout(() => {
          this.sendIndividually(messages);
        }, 5000);
      })
      .then(() => (this.sending = false));
  }

  async sendIndex(messages: Message[]) {
    this.sending = true;
    return this.producer
      .send({
        topic: INDEX_TOPIC,
        messages: messages.map((m) => {
          return {
            key: m.dataset,
            value: JSON.stringify(m),
          };
        }),
      })
      .catch((e) => {
        console.error("SEND ERROR", e.message);
        setTimeout(() => {
          this.sendIndex(messages);
        }, 5000);
      })
      .then(() => (this.sending = false));
  }

  async eachBatch({
    batch,
    resolveOffset,
    heartbeat,
    isRunning,
    isStale,
  }: EachBatchPayload) {
    if (isStale()) {
      return;
    }

    const { messages: remote_messages } = batch;

    bluebird.map(remote_messages, (rmessage) => {
      resolveOffset(rmessage.offset);
      return heartbeat();
    });

    const filtered_messages = await bluebird.filter(
      remote_messages,
      async (rmessage) => {
        return !!rmessage.value;
      }
    );

    const messages = await bluebird.map(filtered_messages, (rmessage) => {
      const payload = rmessage?.value?.toString() as string;
      this.offset = rmessage.offset;
      return JSON.parse(payload);
    });

    await this.recieveCallback(messages);
  }

  async eachMessage({
    // topic,
    // partition,
    message,
  }: EachMessagePayload) {
    const payload = message?.value?.toString() as string;
    this.offset = message.offset;
    const rmessage = JSON.parse(payload) as Message;

    if (rmessage.sender_id === sender_id) return;

    await this.recieveCallback([rmessage]);
  }

  _service() {
    let ref: any;
    const sender = async () => {
      if (this.outgoing_messages.length === 0) {
        ref = setTimeout(sender.bind(this), 10);
        return;
      }

      // const messages = [...this.outgoing_messages];
      this.outgoing_messages = [];

      ref = setTimeout(sender.bind(this), 1);
    };

    ref = setTimeout(sender.bind(this), 1);
    return () => {
      if (ref) clearTimeout(ref);
    };
  }
}

export default KafkaTransport;
