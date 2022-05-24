import { Message } from './index';
export interface RestSyncEndpoint {
  scheme: 'http' | 'https';
  hostname: string;
  port: number;
  url: string;
}

export type MessageCallback = (messages: Message[]) => void;
