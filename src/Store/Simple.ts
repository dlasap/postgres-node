import Sync from "../Core/Sync";
import { Timestamp } from "../Core/Timestamp";
import uuidv4 from "../../utils/uuidv4";
import {
  Message,
  StoreItem,
  StoreAccessCollectionParameters,
  StoreAccessItemParameters,
  StoreAccessByFilterParameters,
  StoreAccessByIndexParameters,
  ISingleFilterCriteriaRequest,
  ISearchParams,
  StoreAccessBaseParameters,
} from "../types";

import PartialSimple from "./PartialSimple";

const { SERVICE_ID = "1" } = process.env;

/*
  Filename: Store.ts
  Class: Store
  Description: 
    Simple Storage Class that implements a Grow-Only Set(GSet) of Last-Write-Wins(LWW) mutations. 
    LWW is evaluated using a merkle tree by parent class. (See class: Sync)
  Usage: Use the class as is or extend implement interfaces to other backing storage.
  Backing Storage: In-Memory(RAM)
*/

class Store extends PartialSimple {
  state?: any;

  /*
    After evaluating the position of the message's timestamp within the merkle tree,
    If the the operation is deemed to be the last update - This apply function  is called.
    Override and implement this function to match the operations of your backing store.
  */

  async applyCreateDatabase(database: string, options?: any): Promise<void> {
    this.state = {
      ...this.state,
      [database]: {},
    };
  }

  async applyCreateTable(
    database: string,
    table: string,
    options?: any
  ): Promise<void> {
    if (!this.state[database]) {
      throw new Error(`Database does not exist`);
    }
    this.state[database] = {
      ...(this.state[database][table] = []),
    };
  }

  // async applyCreateIndex(
  //   database: string,
  //   dataset: string,
  //   column: string
  // ): Promise<void> {
  //   return Promise.resolve();
  // }

  async applyUpdate(msg: Message) {
    const { database, dataset, row, column, value } = msg;

    if (!this.state[database]) {
      throw new Error(`Database does not exist`);
    }

    if (!this.state[database][dataset]) {
      throw new Error(`Table does not exists in Database ${database}`);
    }

    const payload: StoreItem | null = this.state[database][dataset].find(
      (record: StoreItem) => record.id === row
    );

    if (!payload) {
      this.state[database][dataset].push({ id: row, [column]: value });
    } else {
      payload[column] = value;
    }
  }

  async listDatabases() {
    return Object.keys(this.state);
  }

  async listTables(database: string) {
    if (!this.state[database]) throw new Error(`Database does not exist`);

    return Object.keys(Object.keys(this.state[database]));
  }

  // async listIndex(database: string, dataset: string): Promise<string[]> {
  //   return [];
  // }

  /*
    When accessing data, you can still query them as is with the exception of filtering out tombstones.
  */
  async getById(params: StoreAccessItemParameters) {
    const { database, dataset, id } = params;

    if (!this.state[database]) throw new Error(`Database does not exist`);

    if (!this.state[database][dataset])
      throw new Error(`Table does not exist in ${database}`);

    return this.state[database][dataset].find(
      (record: StoreItem) => record.id === id && !!record.tombstone
    );
  }

  async list(params: StoreAccessCollectionParameters) {
    const { database, dataset, order } = params;
    const { start, limit, order_directions, order_key } = order;
    if (!this.state[database]) throw new Error(`Database does not exist`);

    if (!this.state[database][dataset])
      throw new Error(`Table does not exist in ${database}`);

    const requested_range = start + limit;
    const set = this.state[database][dataset];
    const actual_range =
      requested_range < set.length ? requested_range : set.length;

    const ascendingOrder = (a: StoreItem, b: StoreItem) => {
      return a[order_key as string] < b[order_key as string];
    };
    const descendingOrder = (a: StoreItem, b: StoreItem) => {
      return a[order_key as string] > b[order_key as string];
    };

    if (!order_directions && !order_key) {
      return set.slice(start, actual_range);
    }

    return set
      .slice(start, actual_range)
      .sort(
        order_directions === "ASCENDING" ? ascendingOrder : descendingOrder
      );
  }
  async getByIndex(params: StoreAccessByIndexParameters<any>) {
    return this.getByFilter({
      ...params,
      filters: params.indexes,
    });
  }
  async getByFilter(params: StoreAccessByFilterParameters<any>) {
    const { database, dataset, order, filters } = params;
    const { start, limit, order_directions, order_key } = order;
    if (!this.state[database]) throw new Error(`Database does not exist`);

    if (!this.state[database][dataset])
      throw new Error(`Table does not exist in ${database}`);

    const table = this.state[database][dataset];

    const reducerFunction = (total: StoreItem[], key: string) => {
      const matches =
        total.length === 0
          ? table.filter(
              (record: StoreItem) =>
                record[key] === filters[key] && !!record.tombstone
            )
          : total.filter(
              (record: StoreItem) =>
                record[key] === filters[key] && !!record.tombstone
            );

      return matches;
    };

    const ascendingOrder = (a: StoreItem, b: StoreItem) => {
      return a[order_key as string] < b[order_key as string] ? 1 : 0;
    };
    const descendingOrder = (a: StoreItem, b: StoreItem) => {
      return a[order_key as string] > b[order_key as string] ? 1 : 0;
    };

    const requested_range = start + limit;
    const set = Object.keys(filters).reduce(reducerFunction, []);
    const actual_range =
      requested_range < set.length ? requested_range : set.length;

    if (!order_directions && !order_key) {
      return set.slice(start, actual_range);
    }

    return set
      .slice(start, actual_range)
      .sort(
        order_directions === "ASCENDING" ? ascendingOrder : descendingOrder
      );
  }
  async getBySingleFilter(
    params: ISingleFilterCriteriaRequest
  ): Promise<StoreItem[]> {
    return [];
  }
  async search(params: ISearchParams): Promise<StoreItem[]> {
    throw new Error("Search Not Implemented");
    return [];
  }
  async count(_: StoreAccessBaseParameters): Promise<number> {
    return 0;
  }

  async paginatedResults(_: StoreAccessByFilterParameters<any>) {
    return [];
  }
}

export default Store;
