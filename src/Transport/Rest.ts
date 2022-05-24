import axios from 'axios';

import { RestSyncEndpoint, MessageCallback } from '../types';
import Transport from './Transport';

import createLogger from "../../utils/logger";

/*
  Filename: Rest.ts
  Class: Rest
  Description: 
    Rest Transport Implementation of the Message Transport. 
    Transports messagest to peer nodes using REST protocol.
  Usage: 
      Instantiate the class and start sending messages using <transport_object>.send(messages)
      Constructor takes in a list of REST Server Peers.
  Transport: 
    REST (one-way, outgoing)
*/

const { SERVICE_ID = '1', GROUP_ID = '1' } = process.env;
const logger = createLogger("transport:http");

export default class HTTPTransport extends Transport {
  sync_interval: number;
  endpoints: RestSyncEndpoint[];
  stopService?: () => void;

  constructor(endpoints: RestSyncEndpoint[], sync_interval = 5000) {
    super();
    this.endpoints = endpoints;
    this.sync_interval = sync_interval;
    this.enabled = true;
    this.stopService = this._service().bind(this);
  }

  async initialize() {
    logger.info(`Transport Initialized`);
  }

  enable() {
    this.stopService = this._service().bind(this);
    this.enabled = true;
  }

  disable() {
    //@ts-ignore
    this?.stopService();
    this.enabled = false;
  }

  private _service() {
    const interval = setInterval(() => {
      const messages = this.outgoing_messages;

      if (messages.length === 0) return;

      this.endpoints.forEach(async (endpoint) => {
        const { hostname, port, scheme, url } = endpoint;
        const full_url = `${scheme}://${hostname}:${port}${url}`;

        try {
          logger.info(`Transport Sending [${full_url}]`, messages);
          await axios.post(full_url, {
            payload: {
              service_id: SERVICE_ID,
              group_id: GROUP_ID,
              messages,
              client_id: this.clock?.getClock().timestamp.node(),
              merkle: this.clock?.getClock().merkle,
            },
          });

          this.outgoing_messages = [];
        } catch (e) {
          logger.info("Sync Failure");
          this.outgoing_messages = [...messages];
        }
      });
    }, this.sync_interval);

    return () => {
      clearInterval(interval);
    };
  }
}
