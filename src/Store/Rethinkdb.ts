import SimpleStore from "./Simple";
import {
  Message,
  RethinkdbStoreConfig,
  StoreItem,
  StoreAccessCollectionParameters,
  StoreAccessItemParameters,
  StoreAccessByFilterParameters,
  StoreAccessByIndexParameters,
  SystemQueryOperations,
  SystemUpdateOperations,
  StoreAccessOperations,
  ISingleFilterCriteriaRequest,
  ISearchParams,
} from "../types";
import rethinkdbdash, { ConnectOptions, Client } from "rethinkdbdash";
import createLogger from "../../utils/logger";
import { getCode } from "./utils/system_code";
const logger = createLogger("store:rethinkdb");

/**
 * * RethinkDB ORM for CRDT.
 * * Usage: new RethinkdbStore(config as RethinkdbStoreConfig)
 @class RedisStore
 @extends SimpleStore
 @param {object} config typeof RedisStoreConfig
*/
class RethinkdbStore
  extends SimpleStore
  implements
    SystemQueryOperations,
    SystemUpdateOperations,
    StoreAccessOperations
{
  r: Client;
  config: ConnectOptions;

  constructor({ transport, message_store, config }: RethinkdbStoreConfig) {
    super({ transport, message_store });

    this.config = config;
    this.r = rethinkdbdash(config);
  }

  async initialize() {
    super.initialize();
    logger.info(`Store Initialized`);
  }

  async applyCreateDatabase(database: string, options?: any): Promise<void> {
    try {
      await this.r.dbCreate(database).run();
    } catch (e: any) {
      logger.error(e.message);
    }
    return;
  }

  async applyCreateTable(
    database: string,
    table: string,
    _?: unknown
  ): Promise<void> {
    try {
      await this.r.db(database).tableCreate(table).run();
    } catch (e: any) {
      logger.error(e.message);
    }
  }

  async applyCreateIndex(
    database: string,
    dataset: string,
    column: string
  ): Promise<void> {
    try {
      await this.r.db(database).table(dataset).indexCreate(column);
    } catch (e: any) {
      logger.error(e.message);
    }
  }
  async applyUpdate(msg: Message) {
    const {
      database,
      dataset,
      row: id,
      column,
      value,
      system_id = "",
      entity_fields = {},
    } = msg;

    const row = await this.r.db(database).table(dataset).get(id).run();
    let code = await getCode({
      entity_fields,
      dataset,
      system_id,
      column,
      value,
      system_code: value,
    });

    if (!row) {
      await this.r
        .db(database)
        .table(dataset)
        .insert({
          id,
          tombstone: 0,
          [column]: value,
          system_id,
          ...(code && { code }),
        })
        .run();
    } else {
      await this.r
        .db(database)
        .table(dataset)
        .get(id)
        .update({
          [column]: value,
          system_id,
          ...(code && { code }),
        })
        .run();
    }
    return;
  }

  async listDatabases(): Promise<string[]> {
    return this.r.dbList().run();
  }

  async listTables(database: string): Promise<string[]> {
    return this.r.db(database).tableList().run();
  }

  async listIndex(database: string, dataset: string): Promise<string[]> {
    return this.r.db(database).table(dataset).indexList().run();
  }

  /*
    When accessing data, you can still query them as is with the exception of filtering out tombstones.
  */
  async getById(params: StoreAccessItemParameters): Promise<StoreItem | null> {
    const { database, dataset, id } = params;
    return this.r
      .db(database)
      .table(dataset)
      .get(id)
      .run() as unknown as StoreItem | null;
  }

  async list(params: StoreAccessCollectionParameters): Promise<StoreItem[]> {
    const { database, dataset, order } = params;
    const { limit, start, order_key, order_directions } = order;

    let query = this.r.db(database).table(dataset);

    if (!!order_directions && !!order_key) {
      query.orderBy(
        order_directions === "ASCENDING"
          ? this.r.asc(order_key)
          : this.r.desc(order_key)
      );
    }

    return query.skip(start).limit(limit).run() as unknown as Promise<
      StoreItem[]
    >;
  }
  async getByIndex(
    params: StoreAccessByIndexParameters<any>
  ): Promise<StoreItem[]> {
    const { database, dataset, order, indexes } = params;
    const { limit, start, order_key, order_directions } = order;

    const index = Object.keys(indexes);

    if (index.length == 0) {
      return this.list(params);
    }

    if (index.length !== 1) {
      return this.getByFilter({
        ...params,
        filters: indexes,
      });
    }

    const index_name = index[0];
    const index_value = indexes[index_name];

    let query = this.r
      .db(database)
      .table(dataset)
      .getAll(index_value, { index: index_name })
      .filter({
        tombstone: 0,
      });

    if (!!order_directions && !!order_key) {
      query.orderBy(
        order_directions === "ASCENDING"
          ? this.r.asc(order_key)
          : this.r.desc(order_key)
      );
    }

    return query.skip(start).limit(limit).run() as unknown as Promise<
      StoreItem[]
    >;
  }
  async getByFilter(
    params: StoreAccessByFilterParameters<any>
  ): Promise<StoreItem[]> {
    const { database, dataset, order, filters } = params;
    const { limit, start, order_key, order_directions } = order;

    let query = this.r
      .db(database)
      .table(dataset)
      .filter({
        ...filters,
        tombstone: 0,
      });

    if (!!order_directions && !!order_key) {
      query.orderBy(
        order_directions === "ASCENDING"
          ? this.r.asc(order_key)
          : this.r.desc(order_key)
      );
    }

    return query.skip(start).limit(limit).run() as unknown as Promise<
      StoreItem[]
    >;
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
}

export default RethinkdbStore;
