import EventEmitter from 'events';
import Transport from './Transport';
import { Message, MessageCallback } from '../types';

export default class EventTransport extends Transport {
  outgoing_messages: Message[];
  sync_interval: number;
  enabled: boolean;
  emitter: EventEmitter;
  stopService?: () => void;

  constructor(sync_interval: number = 1000) {
    super();
    this.outgoing_messages = [];
    this.sync_interval = sync_interval;

    this.emitter = new EventEmitter();
    this.enabled = true;
    this.stopService = this._service().bind(this);
  }

  getEmmitter() {
    return this.emitter;
  }

  _service() {
    const interval = setInterval(() => {
      const messages = this.outgoing_messages;

      if (messages.length === 0) return;

      this.emitter.emit('message', [...messages]);

      this.outgoing_messages = [];
    });

    return () => {
      clearInterval(interval);
    };
  }
}
