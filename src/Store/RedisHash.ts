import SimpleStore from './Simple';
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
} from '../types';
import Redis from 'ioredis';
import bluebird from 'bluebird';
import createLogger from '../../utils/logger';

const logger = createLogger('store:redis-hash');

function isNumeric(str: string) {
  if (typeof str != 'string') return false; // we only process strings!
  return (
    !isNaN(str as unknown as number) && // use type coercion to parse the _entirety_ of the string (`parseFloat` alone does not do this)...
    !isNaN(parseFloat(str))
  ); // ...and ensure strings of whitespace fail
}

function isEmptyObject(obj: any) {
  return Object.keys(obj).length === 0;
}

function toObject(obj: any) {
  try {
    return JSON.parse(obj);
  } catch (e) {
    return obj;
  }
}

function sanitizeObject(object: any) {
  if (typeof object != 'object') return false;

  return Object.entries(object).reduce((total: any, entry: any[]) => {
    const [key, value] = entry;
    return {
      ...total,
      [key]: isNumeric(value) ? parseInt(value) : toObject(value),
    };
  }, {});
}

function filterMatch<TScheme = StoreItem>(
  object: TScheme,
  object2: Partial<TScheme>
) {
  return Object.entries(object2).reduce(
    (current: boolean, [key, value]: any[]) => {
      return (
        current &&
        (object as unknown as StoreItem)[key][value] ===
          (object2 as unknown as StoreItem)[key][value]
      );
    },
    true
  );
}

/**
 * * Redis ORM for CRDT.
 * * Usage: new RedisHashStore(config as RedisStoreConfig)
 @class RedisHashStore
 @extends SimpleStore
 @param {object} config typeof RedisStoreConfig
*/
class RedisHashStore
  extends SimpleStore
  implements
    SystemQueryOperations,
    SystemUpdateOperations,
    StoreAccessOperations
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

    this.redis_client.on('connect', () => {
      logger.error(`Redis Connected to ${config.host}:${config.port}`);
    });
  }

  async initialize() {
    await super.initialize();
  }
  async applyCreateDatabase(database: string, options?: any): Promise<void> {
    const key = `database:${database}`;
    const config = JSON.stringify({ ...options });

    const exists = await this.redis_client.sismember('databases', key);

    if (exists) {
      logger.error(
        `Database already exists -  ${JSON.stringify(exists, null, 2)}`
      );
    } else {
      await this.redis_client.sadd('databases', key);
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
    const { database, dataset, row: id, column, value = 0 } = msg;
    const table_key = `database:${database}:${dataset}:items`;
    const key = `${database}:${dataset}:item:${id}`;

    // logger.info(
    //   `Applying Change - update - ${dataset} r:${id} c:${column} v:${value}`
    // );

    await this.redis_client.hset(
      key,
      column,
      typeof value === 'object' ? JSON.stringify(value) : value
    );

    await this.redis_client.sadd(table_key, key);

    // logger.info(
    //   `Applied Change - update - ${dataset} r:${id} c:${column} v:${value}`
    // );
  }
  async listDatabases(): Promise<string[]> {
    return this.redis_client.smembers('databases').then((data: string[]) =>
      data.map((key: string) => {
        return key.split(':').pop();
      })
    );
  }
  async listTables(database: string): Promise<string[]> {
    return this.redis_client
      .smembers(`${database}:tables`)
      .then((data: string[]) =>
        data.map((key: string) => {
          return key.split(':').pop();
        })
      );
  }
  async listIndex(database: string, dataset: string): Promise<string[]> {
    return this.redis_client
      .smembers(`${database}:${dataset}:indices`)
      .then((data: string[]) =>
        data.map((key: string) => {
          return key.split(':').pop();
        })
      );
  }
  /*
    When accessing data, you can still query them as is with the exception of filtering out tombstones.
  */
  async getById(params: StoreAccessItemParameters): Promise<StoreItem | null> {
    const { database, dataset, id } = params;
    const key = `${database}:${dataset}:item:${id}`;
    const json = await this.redis_client.hgetall(key);

    if (!json || isEmptyObject(json)) {
      return null;
    }

    const data = sanitizeObject(json);
    if (data.tombstone) {
      return null;
    }

    return sanitizeObject(data);
  }
  async list(params: StoreAccessCollectionParameters): Promise<StoreItem[]> {
    const { database, dataset, order } = params;
    const { start = 0, limit } = order;

    const table_key = `database:${database}:${dataset}:items`;

    return new Promise((resolve) => {
      const results: StoreItem[] = [];
      let items: string[] = [];
      const stream = this.redis_client.sscanStream(table_key);

      const onEachKeyBatch = async (key: string) => {
        const item: any = await this.redis_client.hgetall(key);
        const sanitized_item: StoreItem = sanitizeObject(item);
        if (sanitized_item.tombstone) return;
        results.push(sanitized_item);
      };

      const onStreamClose = async () => {
        const range = start + limit;
        const upper_bound = range > items.length ? items.length : range;

        const key_set: string[] = items.slice(start, upper_bound);

        await bluebird.each(key_set, onEachKeyBatch);

        resolve(results);
      };

      // Scan Keys in Batches
      stream.on('data', (batch_keys: string[]) => {
        stream.pause();
        items = [...items, ...batch_keys];
        stream.resume();
      });

      stream.on('close', onStreamClose);
    });
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

    const table_key = `database:${database}:${dataset}:items`;

    return new Promise((resolve) => {
      const results: StoreItem[] = [];
      let items: string[] = [];
      const stream = this.redis_client.sscanStream(table_key);

      const onStreamClose = async () => {
        const range = start + limit;
        const upper_bound = range > items.length ? items.length : range;
        const key_set: string[] = items.slice(start, upper_bound);

        const onEachKeyBatch = async (key: string) => {
          const item: any = await this.redis_client.hgetall(key);
          const sanitized_item: StoreItem = sanitizeObject(item);
          if (sanitized_item.tombstone) return;
          if (!filterMatch(sanitized_item, filters)) return;
          results.push(sanitized_item);
        };

        await bluebird.each(key_set, onEachKeyBatch);

        resolve(results);
      };

      // Scan Keys in Batches
      stream.on('data', (batch_keys: string[]) => {
        stream.pause();
        items = [...items, ...batch_keys];
        stream.resume();
      });

      stream.on('close', onStreamClose);
    });
  }
  async getBySingleFilter(
    params: ISingleFilterCriteriaRequest
  ): Promise<StoreItem[]> {
    return [];
  }
  async search(params: ISearchParams): Promise<StoreItem[]> {
    throw new Error('Search Not Implemented');
    return [];
  }
}

export default RedisHashStore;
