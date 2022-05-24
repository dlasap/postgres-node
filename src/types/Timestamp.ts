export interface TimestampState {
  millis: number;
  counter: number;
  node: any;
}

export interface TimestampInitOptions {
  maxDrift?: number;
}
