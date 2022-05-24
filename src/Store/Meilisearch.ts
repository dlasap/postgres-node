import SimpleStore from "./Simple";
import {
  Message,
  StoreItem,
  StoreAccessCollectionParameters,
  StoreAccessItemParameters,
  SystemUpdateOperations,
  StoreAccessOperations,
  MeilisearchdbStoreConfig,
  // ElasticConfig,
  StoreAccessByFilterParameters,
  StoreAccessByIndexParameters,
  ElasticSeachOperations,
  // IElasticSearchQuery,
  // IElasticSearchBody,
  ISingleFilterCriteriaRequest,
  ISearchParams,
} from "../types";
// import template from './elastic_templates';

// import {
//     cloneDeep,
//     flattenDeep,
//     map,
//     omit,
//     range,
//     uniqBy,
//     uniqWith,
// } from 'lodash';
import uuidv4 from "../../utils/uuidv4";
// const { insideCircle } = require('geolocation-utils');
// const { SCHEMA_VERSION = 'v1' } = process.env;
// import Bluebird from 'bluebird';

import createLogger from "../../utils/logger";

import must_no_indexed from "./elastic_templates/must_not_indexed";
import default_settings from "./meilisearch/default_settings";
import { excluded_flatten_entities } from "./meilisearch/excluded_flatten_entities";
import { restructureNestedObject, isUUID } from "../../utils/helper";
import { isArray, isObject } from "lodash";

const logger = createLogger("store:elastic");
const deepEqual = require("deep-equal");

import {
  MeiliSearch,
  Document,
  Config as MeilisearchConfig,
  Settings,
  EnqueuedTask,
  Task,
  // Synonyms,
  // StopWords,
  // RankingRules,
  // DistinctAttribute,
  // FilterableAttributes,
  // SortableAttributes,
  // SearchableAttributes,
  // DisplayedAttributes,
} from "meilisearch";
// import settings from './elastic_templates/settings';

class MeilisearchDbStore
  extends SimpleStore
  implements
    SystemUpdateOperations,
    StoreAccessOperations,
    ElasticSeachOperations
{
  config: MeilisearchConfig;
  meilisearch?: MeiliSearch;
  version: number;
  must_not_index: Array<string>;
  default_settings: Settings;
  constructor(store_config: MeilisearchdbStoreConfig) {
    const { transport, message_store, config } = store_config;
    super({
      transport,
      message_store,
      config,
    } as MeilisearchdbStoreConfig);
    this.config = config;
    this.meilisearch = new MeiliSearch(config);
    this.version = 1;
    this.must_not_index = must_no_indexed;
    this.default_settings = default_settings;
  }

  async inititalize() {
    await super.initialize();
    const result = await this.meilisearch?.health();
    logger.info(
      `Initialized Meilisearch @${this.config.host}: ${JSON.stringify(
        result,
        null,
        2
      )}`
    );
  }

  async applyCreateDatabase(database: string, _?: any): Promise<void> {
    try {
      const db_name = database.toLowerCase();
      let index = "databases_list";
      await this.meilisearch?.createIndex(index, {
        primaryKey: "id",
      });
      await this.meilisearch?.index(index).updateFilterableAttributes(["name"]);
      const databases = await this.listDatabases();
      const exists = databases.includes(db_name);
      if (exists) {
        logger.error(
          `Database already exists -  ${JSON.stringify(exists, null, 2)}`
        );
        return;
      } else {
        logger.info(`ADDING DATABASE.... ${index} * ${db_name}`);
        const result = await this.meilisearch?.index(index).addDocuments([
          {
            id: uuidv4(),
            name: db_name,
          },
        ]);
        logger.info(`INFO DB: ${JSON.stringify(result)}`);
      }
    } catch (error: any) {
      logger.error(error.message);
    }
    return;
  }
  async listDatabases(): Promise<string[]> {
    try {
      const databases_result = await this.meilisearch
        ?.index("databases_list")
        .getDocuments();
      const result = databases_result?.map(
        (db: Document) => db?.name
      ) as Array<string>;
      return result.length ? result : [];
    } catch (error: any) {
      return [];
    }
  }
  async applyCreateTable(
    database: string,
    table: string,
    _?: unknown
  ): Promise<void> {
    try {
      const index = `${database + "-" + table}`;
      const existing_tables = await this.listTables(database);
      if (!existing_tables.includes(table))
        await this.setSettings(index, this.default_settings);

      await this.meilisearch?.createIndex(index, {
        primaryKey: "id",
      });
    } catch (e: any) {
      logger.error(e.message);
    }
  }
  async listTables(database: string): Promise<string[]> {
    try {
      const indices_result = await this.meilisearch?.getIndexes();
      const database_map = indices_result?.filter((item: Document) => {
        const uid = item.uid;
        return uid.includes(database);
      });
      const tables = database_map?.map((item: Document) => {
        const db_name = item.uid
          .replace(`${database}-`, "")
          .replace(`_v${this.version}`, "");
        return db_name;
      });
      return tables as Array<string>;
    } catch (e) {
      return [];
    }
  }
  async getIndex(index: string) {
    try {
      return await this.meilisearch?.getIndex(index);
    } catch (error: any) {
      return false;
    }
  }

  async getById(params: StoreAccessItemParameters): Promise<StoreItem | null> {
    try {
      const { database, dataset, id } = params;
      const index = `${database + "-" + dataset}`;
      const result = await this.meilisearch?.index(index).getDocument(id);
      if (result?.tombstone) return {} as unknown as StoreItem;
      return result ?? {};
    } catch (error: any) {
      return {} as unknown as StoreItem;
    }
  }
  async list(
    params: StoreAccessCollectionParameters
  ): Promise<Array<StoreItem>> {
    const { database, dataset, order } = params;
    const { limit = 50, start = 0, order_key, order_directions } = order;
    const index = `${database + "-" + dataset}`;
    let sort_direction = "asc";

    if (!!order_directions && !!order_key) {
      if (["DESCENDING"].includes(order_directions)) {
        sort_direction = "desc";
      }
    }
    const result = await this.meilisearch?.index(index).search("", {
      limit,
      offset: start,
      filter: ["tombstone=0"],
      sort: [`id:${sort_direction}`],
    });

    return result?.hits as StoreItem[];
  }
  async getByIndex(
    params: StoreAccessByIndexParameters<any>
  ): Promise<StoreItem[]> {
    try {
      return this.list(params);
    } catch (e: any) {
      logger.error(`[ERROR]: ${e}`);
      return [];
    }
  }
  async insertMeili(index: string, body: any) {
    try {
      const insert_index = this.meilisearch?.index(index);
      return insert_index?.addDocumentsInBatches([body], 1, {
        primaryKey: "id",
      });
    } catch (e) {
      logger.log("INSERT MEILISEARCH:", e);
    }
  }
  async updateMeili(index: string, body: any) {
    try {
      const update_index = this.meilisearch?.index(index);
      return await update_index?.updateDocuments(body);
    } catch (e) {
      return e;
    }
  }
  async applyUpdate(msg: Message) {
    try {
      const {
        database,
        dataset,
        row: id,
        column,
        value,
        entity_fields = {},
      } = msg;

      // if (this.must_not_index.includes(dataset)) return;

      let index = database + "-" + dataset;

      const result = await this.getIndex(index);

      if (!result) return;

      let restructured_entity_fields = entity_fields;
      const row = (await this.getById({
        id,
        database,
        dataset,
      })) as StoreItem;

      const isExisting = Object.keys(row).length;

      //update filterableAttributes by fields and updating searchableAttributes with fields that are not UUID string type
      if (!excluded_flatten_entities.includes(dataset)) {
        const restructured_obj = await restructureNestedObject(entity_fields);
        //update filterable atrtributes for nested object
        const settings = await this.getSettings(index);
        const fields = Object.keys(restructured_obj);
        await this.meilisearch
          ?.index(index)
          .updateFilterableAttributes([
            ...(settings?.filterableAttributes as Array<string>),
            ...fields,
          ]);
        const additional_searchable_fields = fields.filter((key) => {
          if (
            !settings?.searchableAttributes?.includes(key) &&
            key !== "id" &&
            !key.includes("_ids") &&
            !key.includes(".id")
          ) {
            if (
              key.includes("_id") &&
              (!restructured_obj[key] || isArray(restructured_obj[key]))
            )
              return false;
            return !(key.includes("_id") && isUUID(restructured_obj[key]));
          }
          return false;
        });
        await this.meilisearch
          ?.index(index)
          .updateSearchableAttributes([
            ...(settings?.searchableAttributes as Array<string>),
            ...additional_searchable_fields,
          ]);
      }

      const update_params =
        isExisting && column === "tombstone" && value === 1
          ? {
              id,
              tombstone: 1,
              status: "Deleted",
              version: row!.version + 1,
              updated_date: new Date().getTime(),
            }
          : {
              ...restructured_entity_fields,
              id,
            };

      if (!isExisting) {
        const result = await this.insertMeili(index, {
          ...restructured_entity_fields,
          id,
        });
        if (!result) logger.error(`[ERROR]: ${result}`);
        return;
      }
      await this.updateMeili(index, update_params);
      return;
    } catch (e: any) {
      logger.error(`[ERROR]: `, e);
    }
  }
  //!MUST remove all update filters/sortable when filterable/sortable fields are finalized
  async getByFilter(
    params: StoreAccessByFilterParameters<any>
  ): Promise<StoreItem[]> {
    try {
      const {
        database,
        dataset,
        order = { limit: 50, start: 0 },
        filters,
      } = params;
      const { limit = 50, start = 0, order_key, order_directions } = order;
      const database_index = `${database + "-" + dataset}`;
      const filter_keys = Object.keys(filters);
      const settings = await this.getSettings(database_index);
      const index = this.meilisearch?.index(database_index);

      await index?.updateFilterableAttributes([
        ...(settings?.filterableAttributes as Array<string>),
        ...filter_keys,
      ]);
      let sort_direction = "asc";
      if (!!order_directions && !!order_key) {
        if (["DESCENDING"].includes(order_directions)) {
          sort_direction = "desc";
        }
        if (!settings?.sortableAttributes?.includes(order_key)) {
          await index?.updateSortableAttributes([
            ...(settings?.sortableAttributes as Array<string>),
            order_key,
          ]);
        }
      }
      const mapped_filters = Object.entries(filters).map(
        ([key, value]: any) => `${key}="${value}"`
      );
      console.log("[MEILISEARCH MAPPED FILTERS]:", mapped_filters);

      const result = await index?.search("", {
        limit,
        offset: start,
        filter: [...mapped_filters, "tombstone=0"],
        sort: [`${order_key ? order_key : "id"}:${sort_direction}`],
      });
      return result?.hits as StoreItem[];
    } catch (error: any) {
      logger.info(`GET BY FILTER ERROR: ${error.message}`);

      if (error.message.includes("is not filterable"))
        return await this.getByFilter(params);

      return [];
    }
  }

  async setSettings(index: string, options: Settings) {
    const settings = (await this.meilisearch
      ?.index(index)
      .updateSettings(options)) as unknown as EnqueuedTask;
    const { uid: task_id } = settings;

    const task_status = setInterval(async () => {
      const { status } = (await this.meilisearch
        ?.index(index)
        .getTask(task_id)) as Task;
      if (status == "succeeded") {
        clearInterval(task_status);
        return;
      }
    }, 500);
  }
  async getSettings(index: string) {
    const settings = await this.meilisearch?.index(index).getSettings();
    return settings;
  }
  async isTaskReady(index: string, task_id: number) {
    const checkTask = setInterval(async () => {
      const { status } = (await this.meilisearch
        ?.index(index)
        .getTask(task_id)) as Task;
      if (status == "succeeded") {
        clearInterval(checkTask);
        return true;
      }
    }, 500);
  }
  async getBySingleFilter(
    params: ISingleFilterCriteriaRequest
  ): Promise<StoreItem[]> {
    return [];
  }
  async search(params: ISearchParams): Promise<StoreItem[]> {
    const { database, dataset, search, order } = params;
    const { limit, start } = order;
    const index = `${database + "-" + dataset}`;

    const partial_result = await this.meilisearch
      ?.index(index)
      .search(search, { limit, offset: start });
    return partial_result?.hits as Array<StoreItem>;
  }
}

export default MeilisearchDbStore;
