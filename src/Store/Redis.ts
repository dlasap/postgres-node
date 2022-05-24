import SimpleStore from "./Simple";
import {
  Message,
  RedisStoreConfig,
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
  ISystemCode,
  IMigrationOperations,
  IPaginationParams,
  StoreAccessBaseParameters,
} from "../types";
import Redis from "ioredis";
import bluebird from "bluebird";
import createLogger from "../../utils/logger";
import sha1 from "sha1";
import os from "os";
import { getCode } from "./utils/system_code";

const { FILTER_CACHE_EXPIRY_MINUTES = "5", SYSTEM_CODE_KEY = "" } = process.env;

const matchSearchText = require("./utils/matchSearchText");
const matchSingleFilter = require("./utils/matchSingleFilter");
const chunkify = require("./utils/chunkify");
const logger = createLogger("store:redis");

const createCacheKey = (params: ISingleFilterCriteriaRequest): string => {
  const { database, dataset, filter } = params;

  return `${database}:${dataset}:filter:${sha1(
    JSON.stringify({ database, dataset, filter })
  )}`;
};

const createSearchCacheKey = (params: ISearchParams): string => {
  const { database, dataset, search } = params;

  return `${database}:${dataset}:search:${sha1(
    JSON.stringify({ database, dataset, search })
  )}`;
};

const matchFilterSearchKeys = (keys: Array<string>): Array<string> =>
  keys.filter((key) => key.includes("filter") || key.includes("search"));

/**
 * * Redis ORM for CRDT.
 * * Usage: new RedisStore(config as RedisStoreConfig)
 @class RedisStore
 @extends SimpleStore
 @param {object} config typeof RedisStoreConfig
*/
class RedisStore
  extends SimpleStore
  implements
    SystemQueryOperations,
    SystemUpdateOperations,
    StoreAccessOperations,
    IMigrationOperations
{
  redis_config: RedisStoreConfig;
  redis_client?: any; // Broken Type definition in IORedis
  constructor(store_config: RedisStoreConfig) {
    const { transport, message_store, config } = store_config;
    super({
      transport,
      message_store: message_store,
    } as RedisStoreConfig);

    this.redis_config = store_config;

    //@ts-ignore
    this.redis_client = new Redis(config as Redis.RedisOptions);
    this.redis_client.on("connect", () => {
      logger.info(`Redis Connected to ${config.host}:${config.port}`);
    });
  }

  async initialize() {
    await super.initialize();
  }

  async applyCreateDatabase(database: string, options?: any): Promise<void> {
    const key = `database:${database}`;
    const config = JSON.stringify({ ...options });

    const exists = await this.redis_client.sismember("databases", key);

    if (exists) {
      logger.error(
        `Database already exists -  ${JSON.stringify(exists, null, 2)}`
      );
    } else {
      await this.redis_client.sadd("databases", key);
      await this.redis_client.set(key, config);
    }
  }

  async applyCreateTable(
    database: string,
    table: string,
    options?: any
  ): Promise<void> {
    const key = `database:${database}:${table}`;
    const config = JSON.stringify({ ...options });

    const exists = await this.redis_client.sismember(`${database}:tables`, key);

    if (exists) {
      // throw new Error(``);
      logger.error(`Table already exists - ${JSON.stringify(exists, null, 2)}`);
    } else {
      await this.redis_client.sadd(`${database}:tables`, key);
      await this.redis_client.set(key, config);
    }
  }

  async applyCreateIndex(
    database: string,
    dataset: string,
    column: string
  ): Promise<void> {
    const key = `database:${database}:${dataset}:${column}`;
    const config = JSON.stringify({});

    const exists = await this.redis_client.sismember(
      `${database}:${dataset}:indices`,
      key
    );

    if (exists) {
      logger.error(`Index already exists ${JSON.stringify(exists, null, 2)}`);
    }

    await this.redis_client.sadd(`${database}:${dataset}:indices`, key);
    await this.redis_client.set(key, config);
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
    const key = `${database}:${dataset}:item:${id}`;
    const row = await this.redis_client?.get(key);
    if (!SYSTEM_CODE_KEY) logger.error(`SYSTEM_CODE_KEY is empty.`);

    let code = await getCode({
      entity_fields,
      dataset,
      system_id,
      column,
      value,
      system_code: value,
    });

    if (!row) {
      // logger.info(
      //   `Applying Change - insert - ${dataset} r:${id} c:${column} v:${value}`
      // );

      return this.redis_client?.set(
        key,
        JSON.stringify({
          id: `${id}`,
          [column]: value,
          system_id,
          tombstone: 0,
          ...(code && { code }),
        })
      );
    }

    // logger.info(
    //   `Applying Change - update - ${dataset} r:${id} c:${column} v:${value}`
    // );

    await this.redis_client?.set(
      key,
      JSON.stringify({
        ...JSON.parse(row),
        [column]: value,
        system_id,
        ...(code && { code }),
      })
    );

    // Start of Cache Expiration for updates
    // const cache_filter_patterns = `${database}:${dataset}:*`;

    // const keys = await this.redis_client.keys(
    //   cache_filter_patterns
    // );

    // const matched_keys = matchFilterSearchKeys(keys)
    // console.log(`matchFilterSearchKeys`, matchFilterSearchKeys.length, matched_keys.slice(0,10))
    // Bluebird.map(matched_keys, (key) =>
    //   this.redis_client.delete(key)
    // );

    // End of Cache Expiry Section
    return;
  }

  async count({ database, dataset }: StoreAccessBaseParameters): Promise<any> {
    const results = await this.redis_client.keys(
      `${database}:${dataset}:item:*`
    );
    return {
      count: results.length,
    };
  }

  async paginatedResults(params: StoreAccessByFilterParameters<any>) {
    return this.getByFilter(params) as unknown as any;
  }

  async listDatabases(): Promise<string[]> {
    return this.redis_client.smembers("databases").then((data: string[]) =>
      data.map((key: string) => {
        return key.split(":").pop();
      })
    );
  }

  async listTables(database: string): Promise<string[]> {
    return this.redis_client
      .smembers(`${database}:tables`)
      .then((data: string[]) =>
        data.map((key: string) => {
          return key.split(":").pop();
        })
      );
  }

  async listIndex(database: string, dataset: string): Promise<string[]> {
    return this.redis_client
      .smembers(`${database}:${dataset}:indices`)
      .then((data: string[]) =>
        data.map((key: string) => {
          return key.split(":").pop();
        })
      );
  }

  /*
    When accessing data, you can still query them as is with the exception of filtering out tombstones.
  */
  async getById(params: StoreAccessItemParameters): Promise<StoreItem | null> {
    const { database, dataset, id } = params;
    const key = `${database}:${dataset}:item:${id}`;

    const json = JSON.parse(await this.redis_client.get(key));

    if (!json) {
      return null;
    }

    if (json.tombstone) {
      return null;
    }

    return json;
  }

  async list(params: StoreAccessCollectionParameters): Promise<StoreItem[]> {
    const { database, dataset, order } = params;
    const { start = 0, limit } = order;

    const items: string[] = await this.redis_client.keys(
      `${database}:${dataset}:item:*`
    );

    const range = start + limit;
    const upper_bound = range > items.length ? items.length : range;

    const key_set = items.slice(start, upper_bound);

    const current_set = await bluebird.map(key_set, async (key: string) => {
      const record = await this.redis_client.get(key);
      return JSON.parse(record) as unknown as StoreItem;
    });

    return bluebird.filter(
      current_set,
      (item: StoreItem) => item.tombstone !== 1
    );
  }
  async getByIndex(
    params: StoreAccessByIndexParameters<any>
  ): Promise<StoreItem[]> {
    return this.getByFilter({
      ...params,
      filters: params.indexes,
    });
  }
  async getByFilter(
    params: StoreAccessByFilterParameters<any>
  ): Promise<StoreItem[]> {
    const { database, dataset, order, filters } = params;
    const { start = 0, limit } = order;

    const items: string[] = await this.redis_client.keys(
      `${database}:${dataset}:item:*`
    );

    const range = start + limit;
    const upper_bound = range > items.length ? items.length : range;
    // const key_set = items.slice(start, upper_bound);

    const records: StoreItem[] = await bluebird.map(
      items,
      async (key: string) => {
        const record = await this.redis_client.get(key);
        return JSON.parse(record) as unknown as StoreItem;
      }
    );

    const data = await bluebird
      .filter(records, (record: StoreItem) => {
        const filter_evaluation = Object.entries(filters).reduce(
          (total, filter) => {
            const [key, value] = filter;
            return total && record[key] === value && record.tombstone !== 1;
          },
          true
        );

        return filter_evaluation;
      })
      .then((items) => items.slice(start, upper_bound));

    return data;
  }

  async getBySingleFilter(
    params: ISingleFilterCriteriaRequest
  ): Promise<StoreItem[]> {
    const {
      database,
      dataset,
      filter,
      order = {
        start: 0,
        limit: 50,
        order_directions: "ASCENDING",
        order_key: "id",
      },
    } = params;
    const {
      start = 0,
      limit = 50,
      order_key = "created_date",
      order_directions = "DESCENDING",
    } = order;

    if (start < 0) {
      return [];
    }

    const cache_key = createCacheKey(params);

    const cache_set = await this.redis_client.get(cache_key);

    // Cache Miss Event = re-evaluate filter.
    // if (!cache_set) {
    const items: string[] = await this.redis_client.keys(
      `${database}:${dataset}:item:*`
    );

    const chunks: Array<Array<string>> = await chunkify(
      items,
      Math.floor(os.cpus().length / 2) || 1
    );

    const mapped_data = await bluebird.map<string[], Array<StoreItem>>(
      chunks,
      async (chunk) => {
        const records: StoreItem[] = await bluebird.map(
          chunk,
          async (key: string) => {
            const record = await this.redis_client.get(key);

            return JSON.parse(record) as unknown as StoreItem;
          }
        );
        return matchSingleFilter(records, filter);
      },
      {
        concurrency: chunks.length,
      }
    );

    const data = await bluebird
      .reduce<Array<StoreItem>, Array<StoreItem>>(
        mapped_data,
        async (total, items) => [...total, ...items],
        []
      )
      .then((records: Array<StoreItem>) =>
        records.sort((a, b) => {
          const a_resolved_value = a[order_key] || a.attribute[order_key];
          const b_resolved_value = b[order_key] || b.attribute[order_key];
          if (order_directions === "ASCENDING") {
            return a_resolved_value - b_resolved_value;
          }
          return b_resolved_value - a_resolved_value;
        })
      );

    // this.redis_client.set(
    //   cache_key,
    //   JSON.stringify(data),
    //   'EX',
    //   1000 * 60 * parseInt(FILTER_CACHE_EXPIRY_MINUTES)
    // );

    return data.slice(start, start + limit);
    //  } else {
    //    const data = JSON.parse(cache_set);
    //    return data.slice(start, start + limit);
    //   }
  }
  async search(params: ISearchParams) {
    const {
      database,
      dataset,
      search,
      order = {
        start: 0,
        limit: 50,
        order_directions: "DESCENDING",
        order_key: "created_date",
      },
    } = params;
    const {
      start = 0,
      limit = 50,
      order_key = "created_date",
      order_directions = "DESCENDING",
    } = order;

    if (start < 0) {
      return [];
    }

    const cache_key = createSearchCacheKey(params);

    const cache_set = await this.redis_client.get(cache_key);

    // Cache Miss Event = re-evaluate filter.
    //if (!cache_set) {
    const items: string[] = await this.redis_client.keys(
      `${database}:${dataset}:item:*`
    );

    const chunks: Array<Array<string>> = await chunkify(
      items,
      Math.floor(os.cpus().length / 2) || 1
    );

    const mapped_data = await bluebird.map<string[], Array<StoreItem>>(
      chunks,
      async (chunk, index) => {
        const records: StoreItem[] = await bluebird.map(
          chunk,
          async (key: string) => this.redis_client.get(key)
        );
        const data = await matchSearchText(records, search);
        return data;
      },
      {
        concurrency: chunks.length,
      }
    );

    const data = await bluebird
      .reduce<Array<StoreItem>, Array<StoreItem>>(
        mapped_data,
        async (total, items) => [...total, ...items],
        []
      )
      .then((records: Array<StoreItem>) =>
        records.sort((a, b) => {
          const a_resolved_value = a[order_key] || a.attribute[order_key];
          const b_resolved_value = b[order_key] || b.attribute[order_key];
          if (order_directions === "ASCENDING") {
            return a_resolved_value - b_resolved_value;
          }
          return b_resolved_value - a_resolved_value;
        })
      );

    // this.redis_client.set(
    //   cache_key,
    //   JSON.stringify(data),
    //   'EX',
    //   1000 * 60 * parseInt(FILTER_CACHE_EXPIRY_MINUTES)
    // );

    return data.slice(start, start + limit);
    // } else {
    //   const data = JSON.parse(cache_set);
    //   return data.slice(start, start + limit);
    //}
  }
  async getVehicleRateByClass(
    params: StoreAccessByFilterParameters<any>
  ): Promise<StoreItem[]> {
    return [];
  }
  async getByMultipleFilter(
    params: StoreAccessByFilterParameters<any>
  ): Promise<StoreItem[]> {
    return [];
  }
  async getNearbyLocation(
    params: StoreAccessByFilterParameters<any>
  ): Promise<StoreItem[]> {
    return [];
  }
  async getMapping(index: string): Promise<Record<string, any>> {
    return {};
  }
}

export default RedisStore;
