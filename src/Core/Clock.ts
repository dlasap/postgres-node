import { MutableTimestamp, Timestamp } from "./Timestamp";
import uuidv4 from "../../utils/uuidv4";

export class ClockPrototype {
  timestamp: MutableTimestamp;
  merkle: any;
  constructor(timestamp: MutableTimestamp, merkle: any = {}) {
    this.timestamp = timestamp;
    this.merkle = merkle;
  }
}

class Clock extends ClockPrototype {
  constructor(timestamp: MutableTimestamp, merkle: any) {
    super(timestamp, merkle);
  }

  static makeClock(timestamp: MutableTimestamp, merkle: any = {}) {
    return { timestamp: MutableTimestamp.from(timestamp), merkle };
  }

  setClock(clock: Clock) {
    this.merkle = clock.merkle;
    this.timestamp = clock.timestamp;
  }

  getClock() {
    return {
      merkle: this.merkle,
      timestamp: this.timestamp,
    };
  }

  static deserializeClock(clock: string) {
    const data: Clock = JSON.parse(clock);
    return {
      //@ts-ignore
      timestamp: MutableTimestamp.from(MutableTimestamp.parse(data.timestamp)),
      merkle: data.merkle,
    };
  }

  static serializeClock(clock: Clock) {
    return JSON.stringify({
      timestamp: clock.timestamp.toString(),
      merkle: clock.merkle,
    });
  }

  static makeClientId() {
    return uuidv4().replace(/-/g, "").slice(-16);
  }
}

export default Clock;
