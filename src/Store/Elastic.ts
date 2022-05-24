import SimpleStore from "./Simple";
import {
  Message,
  StoreItem,
  StoreAccessCollectionParameters,
  StoreAccessItemParameters,
  SystemUpdateOperations,
  StoreAccessOperations,
  ElasticdbStoreConfig,
  ElasticConfig,
  StoreAccessByFilterParameters,
  StoreAccessByIndexParameters,
  ElasticSeachOperations,
  IElasticSearchQuery,
  IElasticSearchBody,
  ISingleFilterCriteriaRequest,
  ISearchParams,
  StoreAccessBaseParameters,
  IMigrationOperations,
} from "../types";
import template from "./elastic_templates";

import createLogger from "../../utils/logger";
import ElasticSearch from "elasticsearch";
import Bluebird from "bluebird";

import {
  cloneDeep,
  flattenDeep,
  map,
  omit,
  range,
  uniqBy,
  uniqWith,
} from "lodash";
import uuidv4 from "../../utils/uuidv4";
import must_no_indexed from "./elastic_templates/must_not_indexed";
import { getCode } from "./utils/system_code";
const { insideCircle } = require("geolocation-utils");
const { SCHEMA_VERSION = "v1" } = process.env;

const logger = createLogger("store:elastic");
const deepEqual = require("deep-equal");
class ElasticDbStore
  extends SimpleStore
  implements
    SystemUpdateOperations,
    StoreAccessOperations,
    ElasticSeachOperations,
    IMigrationOperations
{
  config: ElasticConfig;
  elastic?: any;
  version: number;
  must_not_index: string[];
  constructor(store_config: ElasticdbStoreConfig) {
    const { transport, message_store, config } = store_config;
    super({
      transport,
      message_store,
      config,
    } as ElasticdbStoreConfig);
    this.config = config;
    this.elastic = new ElasticSearch.Client(config);
    this.version = 1;
    this.must_not_index = must_no_indexed;
  }

  async initialize() {
    await super.initialize();
    const result = await this.elastic.cluster.health();
    logger.info(
      `Initialized Elastic Search: ${JSON.stringify(result, null, 2)}`
    );
  }

  async getMapping(index: string): Promise<Record<string, any>> {
    return this.elastic?.indices.getMapping({ index }).catch(() => ({}));
  }

  async applyCreateDatabase(database: string, _?: any): Promise<void> {
    const db_name = database.toLowerCase();
    const databases = await this.listDatabases();
    const exists = databases.includes(db_name);
    if (exists) {
      logger.error(
        `Database already exists -  ${JSON.stringify(exists, null, 2)}`
      );
      return;
    } else {
      await this.insertElastic(db_name, {
        id: uuidv4(),
        name: db_name,
      });
    }
  }

  async applyCreateTable(
    database: string,
    table: string,
    _?: unknown
  ): Promise<void> {
    if (this.must_not_index.includes(table)) return;

    const name = database + "-" + table;
    const mappings = cloneDeep(template["mappings"]);

    await this.elastic?.indices.putTemplate({
      name,
      body: {
        template: name + "_v*",
        settings: template.settings,
        mappings,
        aliases: {
          [name]: {},
        },
        version: this.version,
      },
    });
    const index = name + "_v" + this.version;

    // create index
    try {
      await this.elastic?.indices.create({ index });
    } catch (e) {
      logger.error(e);
    }
    // refresh index to make sure index fully operational
    await this.elastic?.indices.refresh({ index });
  }

  async applyUpdate(msg: Message) {
    // @ts-ignore !TODO: Entity fields from Jeff
    const {
      database,
      dataset,
      row: id,
      entity_fields = {},
      system_id = "",
      column,
      value,
    } = msg;

    if (this.must_not_index.includes(dataset)) return;

    let index = database + "-" + dataset;

    const result = await this.elastic.indices.get({
      index,
    });

    if (result.status === 404) return;

    const existing_data = (await this.getById({
      id,
      database,
      dataset,
    }).catch((e) => logger.error(e))) as StoreItem;
    try {
      let code = await getCode({
        entity_fields,
        dataset,
        system_id,
        column,
        value,
        system_code: entity_fields.system_code,
      });

      if (!existing_data) {
        const result = await this.insertElastic(index, {
          ...entity_fields,
          id,
          system_id,
          ...(code && { code }),
        });
        if (!result) logger.error(`[ERROR]: ${result}`);
        return;
      }

      const query: IElasticSearchQuery = {
        bool: {
          // @ts-ignore !TODO: Entity must from Jeff
          must: [
            {
              term: {
                _index: index,
              },
            },
            {
              term: {
                _id: id,
              },
            },
          ],
        },
      };

      await this.updateElasticByQuery(index, query, {
        ...entity_fields,
        id,
        system_id,
        ...(code && { code }),
      });
      return;
    } catch (e: any) {
      logger.error(`[ERROR]: `, e);
    }
  }

  async count({ database, dataset }: StoreAccessBaseParameters): Promise<any> {
    const index = `${database}-${dataset}`;
    return this.elastic
      ?.search({
        index,
        body: {
          track_total_hits: true,
          size: 0,
        },
      })
      .then((e: any) => ({
        count: e.hits.total.value,
      }));
  }

  async paginatedResults(params: StoreAccessByFilterParameters<any>) {
    // TODO: use search after for pagination
    return [];
  }

  async listDatabases(): Promise<string[]> {
    const index = `*_db_*`;

    const query = {
      bool: {
        must: {
          term: {
            _index: index,
          },
        },
        must_not: {
          term: {
            _index: `*_v${this.version}-*`,
          },
        },
      },
    };

    const {
      hits: { hits },
    } = await this.elastic?.search({
      // index,
      body: {
        query,
        size: 50,
        track_total_hits: true,
      },
    });

    return hits.map(({ _index = "" }) => _index);
  }

  async listDatabasesGlobalSearch(
    version: string = SCHEMA_VERSION
  ): Promise<string[]> {
    const index = `*_db_${version}`;

    const query = {
      bool: {
        must: {
          term: {
            _index: index,
          },
        },
      },
    };

    const {
      hits: { hits },
    } = await this.elastic?.search({
      body: {
        query,
        size: 50,
        track_total_hits: true,
      },
    });

    return hits.map(({ _index = "" }) => _index);
  }

  async listTables(database: string): Promise<string[]> {
    // List all elastic indexes
    const index = `${database}-*`;
    try {
      const result = await this.elastic.indices
        .get({
          index,
        })
        .then((response: any) =>
          Object.keys(response).map((str: string) =>
            str.replace(`${database}-`, "").replace(`_v${this.version}`, "")
          )
        );

      return result;
    } catch (e) {
      return [];
    }
  }

  /*
    When accessing data, you can still query them as is with the exception of filtering out tombstones.
  */
  async getById(params: StoreAccessItemParameters): Promise<StoreItem | null> {
    const { database, dataset, id } = params;
    const index = database + "-" + dataset;
    const query_params = {
      bool: {
        must: [
          {
            term: {
              id,
            },
          },
          {
            term: {
              tombstone: 0,
            },
          },
        ],
      },
    };
    try {
      const result: any = await this.searchAllElastic({
        index,
        query: query_params,
      });

      return result.data[0];
    } catch (err) {
      logger.error(`[ERROR]: ${err}`);
      return null;
    }
  }

  async list(params: StoreAccessCollectionParameters): Promise<StoreItem[]> {
    const { database, dataset, order } = params;
    const { limit = 50, start = 0, order_key, order_directions } = order;

    const index = database + "-" + dataset;
    let sort_direction = "asc";
    let body = {
      from: start,
      size: limit,
      query: {
        bool: {
          must: [
            {
              term: {
                tombstone: 0,
              },
            },
          ],
        },
      },
    };

    if (!!order_directions && !!order_key) {
      if (["DESCENDING"].includes(order_directions)) {
        sort_direction = "desc";
      }
    }
    try {
      const {
        hits: { hits },
      } = await this.elastic?.search({
        index,
        body: {
          ...body,
          track_total_hits: true,
          sort: [
            {
              [order_key + ""]: {
                unmapped_type: "long",
                order: sort_direction,
              },
            },
          ],
        },
      });

      return hits.map((hit = { _source: {} }) => hit._source);
    } catch (e: any) {
      logger.error(`[ERROR]: ${e}`);
      return [];
    }
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

  async getByFilter(
    params: StoreAccessByFilterParameters<any>
  ): Promise<StoreItem[]> {
    const {
      database,
      dataset,
      order,
      filters,
      advanced_filter = [],
      is_or = true,
      // @ts-ignore only use for auto-generate code resolver
      allow_deleted_status = false,
    } = params;

    const { limit, start, order_key, order_directions } = order;

    const index = database + "-" + dataset;
    logger.info(`Elasticsearch [GET BY FILTER] [index]: ${index}`);
    let sort_direction = "asc";

    const should: any = [];
    const must = await Bluebird.map(Object.entries(filters), ([key, val]) => {
      return {
        term: {
          [key]: val,
        },
      };
    });
    const must_not = [];
    if (!allow_deleted_status) {
      must_not.push({
        term: {
          status: "Deleted",
        },
      });
      must_not.push({
        term: {
          tombstone: 1,
        },
      });
    }
    if (advanced_filter.length) {
      advanced_filter.map((e: any) => {
        const { column, operator, value } = e;
        let arr = is_or ? should : must;
        if (operator === "equal") {
          arr.push({
            terms: {
              [column]: Array.isArray(value) ? value : [value],
            },
          });
        } else if (operator === "contains") {
          if (value.trim().toLowerCase().includes(" ")) {
            arr.push({
              multi_match: {
                fields: [`${column}.edge_ngram`],
                query: value,
                type: "phrase_prefix",
              },
            });
          } else {
            arr.push({
              wildcard: {
                [`${column}.edge_ngram`]: {
                  value: `*${value}*`,
                },
              },
            });
          }
        } else if (operator === "does_not_contain") {
          must_not.push({
            query_string: {
              fields: [`${column}.edge_ngram`],
              query: `*${value}*`,
              default_operator: "AND",
            },
          } as any);
        } else if (operator === "starts_with") {
          arr.push({
            query_string: {
              fields: [`${column}.prefix`],
              query: `${value}`,
              default_operator: "AND",
            },
          });
        } else if (operator === "is_between") {
          arr.push({
            range: {
              [column]: {
                gte: value[0],
                lte: value[1],
              },
            },
          });
        }
      });
    }

    let body = {
      query: {
        bool: {
          must,
          must_not,
          should,
        },
      },
      from: start,
      size: limit,
    };

    if (!!order_directions) {
      if (["DESCENDING"].includes(order_directions)) {
        sort_direction = "desc";
      }
    }
    try {
      const {
        hits: { hits },
      } = await this.elastic?.search({
        index,
        body: {
          ...body,
          track_total_hits: true,
          sort: [
            {
              [order_key + ""]: {
                unmapped_type: "long",
                order: sort_direction,
              },
            },
          ],
        },
      });
      return hits.map((hit = { _source: {} }) => hit._source);
    } catch (e: any) {
      logger.error(`[ERROR]: ${e}`);
      return [];
    }
  }

  async insertElastic(index: string, body: IElasticSearchBody) {
    try {
      return this.elastic?.index({
        id: body.id,
        index,
        body,
        type: "_doc",
      });
    } catch (e) {
      return e;
    }
  }

  async updateElasticByQuery(
    index: string,
    query: IElasticSearchQuery,
    params: any
  ) {
    const source = Object.keys(params).reduce((curr, key) => {
      return (curr += `ctx._source.${key}=params.${key};`);
    }, "");
    try {
      const result = await this.elastic?.updateByQuery({
        // id: params.id,
        index,
        waitForCompletion: true,
        body: {
          query,
          script: {
            source,
            params,
          },
        },
        type: "_doc",
        conflicts: "proceed",
      });

      return result;
    } catch (e) {
      logger.error(`[ERROR]: ${e}`);
      return;
    }
  }

  async searchAllElastic({
    index,
    fields = [],
    query,
    size,
    sort_column,
    sort_direction,
  }: any) {
    logger.info(`Elasticsearch Query: ${JSON.stringify(query)}`);
    const getValuesAndCount: any = (search_after: any) => {
      return this.elastic
        .search({
          index,
          body: {
            query,
            ...(fields.length && { _source: fields }),
            ...(search_after && { search_after }),
            track_total_hits: true,
            sort: [
              {
                [`${sort_column}.sort`]: {
                  unmapped_type: "long",
                  order: sort_direction,
                },
              },
            ],
            size: size || 10000,
          },
        })
        .then(({ hits }: any) => hits);
    };
    const { hits, total } = await getValuesAndCount();
    let data = [...hits];

    if (total && data.length < total && !size) {
      const PER_ITERATION = 100;
      const iterations = (total - hits.length) / PER_ITERATION + 1;
      await Bluebird.mapSeries(range(0, iterations), async () => {
        const { sort: search_after } = data[data.length - 1];
        const { hits: next_result } = await getValuesAndCount(search_after);
        data = data.concat(next_result);
      });
    }

    return {
      data: data.map((e) => e._source),
      count: total,
    };
  }

  searchBuilder(search = [], must: any, field: any) {
    search = Array.isArray(search) ? search : [search];

    if (!search.length) return null;

    const should: object[] = [];

    search.forEach((value: any) => {
      value = value
        .trim()
        .split("")
        .map((each: string) => (each.match(/[^a-zA-Z\d\s-]+/i) ? " " : each))
        .join("")
        .toLowerCase();

      if (value.includes(" ")) {
        should.push({
          multi_match: {
            fields: [`${field}.edge_ngram`],
            query: value,
            type: "phrase_prefix",
          },
        });
      } else {
        should.push({
          wildcard: {
            [`${field}.edge_ngram`]: {
              value: `*${value}*`,
            },
          },
        });
      }
    });

    return must.push({
      bool: {
        should,
      },
    });
  }

  async globalSearch(params: any) {
    try {
      const {
        config: {
          // TODO: cannot desctruct from config > config
          type: type_params,
          excluded_entities: excluded_entities_params = [],
          excluded_search_fields: excluded_search_fields_params = [],
          entities: entities_params = [],
          search_fields = [], // combination of all fields from matching entities
          size = 50,
          sort_column = "created_date",
          sort_direction = "desc", // this is for elastic search
          advanced_filter = [],
          is_or = true,
        },
        schema_version = SCHEMA_VERSION,
      } = params;
      logger.info(
        `[GlobalSearch] - [PARAMS]: ${JSON.stringify(params, null, 2)}`
      );
      const { search } = params;
      // TODO - transfer this to application_config for global search
      // TODO - get config
      // TODO - specify all possible config in Initializer
      // TODO - refactor global search base on what type of search
      // Variable for change: databases
      let databases = await this.listDatabasesGlobalSearch(schema_version);
      logger.info(`Elasticsearch databases: ${databases}`);
      // const excluded_db_params = excluded_databases.length ? excluded_databases.map((e: string) => `${e}_db_v1`) : []
      // databases = databases.filter(e => !excluded_db_params.includes(e))
      // Variable for change: entity
      const excluded_entities = excluded_entities_params;

      // Variable for change: fields
      const excluded_search_fields = excluded_search_fields_params;

      // Retrieve all entities from databases
      const entities = await Bluebird.map(
        databases,
        async (database: string) => {
          // const tables = await this.listTables(database);
          const index_format = database;
          const result = await this.elastic.indices
            .get({
              index: `${index_format}-*`,
            })
            .then(async (e: any) => {
              let indices = Object.keys(e)
                .map((val) =>
                  val
                    .replace(`${index_format}-`, "")
                    .replace(`_${SCHEMA_VERSION}`, "")
                )
                .filter((e) => !excluded_entities.includes(e));
              indices = !["search_menu", "preference"].includes(type_params)
                ? indices
                : entities_params;

              logger.info(`Elasticsearch indices: ${indices}`);

              const result = await Bluebird.map(
                indices,
                async (index: string) => {
                  logger.info(`Elasticsearch index: ${index}`);
                  const entity_fields = !search_fields.length
                    ? await this.elastic
                        .search({
                          index: `${index_format}-${index}`,
                          body: { from: 0, size: 1 },
                        })
                        .then(({ hits: { hits } }: any) =>
                          map(hits, (e) =>
                            Object.keys({ ...e._source }).filter(
                              (res) => !excluded_search_fields.includes(res)
                            )
                          )
                        )
                        .catch((_: any) => [])
                    : search_fields;

                  logger.info(`Elasticsearch Entities: ${entity_fields}`);
                  // if (!entity_fields.length) return {}

                  return {
                    index: `${index_format}-${index}`,
                    entity: index,
                    search_fields: flattenDeep(entity_fields),
                  };
                }
              );

              logger.info(
                `Elasticsearch Entity Fields: ${JSON.stringify(
                  result,
                  null,
                  2
                )}`
              );

              return result;
            });

          return result;
        }
      ).then((e) => flattenDeep(e));

      const entities_value = await Bluebird.map(
        entities,
        async (entity: any) => {
          const { index, search_fields, entity: field_entity } = entity;

          const search_result = await Bluebird.map(
            search_fields,
            async (e: any) => {
              const should: any = [];
              const must: any = [];
              const must_not = [
                {
                  term: {
                    status: "Deleted",
                  },
                },
                {
                  term: {
                    tombstone: 1,
                  },
                },
              ];

              if (advanced_filter.length) {
                advanced_filter.map((e: any) => {
                  const { column, operator, value } = e;
                  let arr = is_or ? should : must;
                  if (operator === "deep_equal") {
                    // Used for deep-equality (exact value) query
                    must.push({
                      terms: {
                        [column]: Array.isArray(value) ? value : [value],
                      },
                    });
                  } else if (operator === "equal") {
                    arr.push({
                      term: {
                        [column]: value,
                      },
                    });
                  } else if (operator === "contains") {
                    if (value.trim().toLowerCase().includes(" ")) {
                      arr.push({
                        multi_match: {
                          fields: [`${column}.edge_ngram`],
                          query: value,
                          type: "phrase_prefix",
                        },
                      });
                    } else {
                      arr.push({
                        wildcard: {
                          [`${column}.edge_ngram`]: {
                            value: `*${value}*`,
                          },
                        },
                      });
                    }
                  } else if (operator === "does_not_contain") {
                    must_not.push({
                      query_string: {
                        fields: [`${column}.edge_ngram`],
                        query: `*${value}*`,
                        default_operator: "AND",
                      },
                    } as any);
                  } else if (operator === "starts_with") {
                    arr.push({
                      query_string: {
                        fields: [`${column}.prefix`],
                        query: `${value}`,
                        default_operator: "AND",
                      },
                    });
                  } else if (operator === "is_between") {
                    arr.push({
                      range: {
                        [column]: {
                          gte: value[0],
                          lte: value[1],
                        },
                      },
                    });
                  }
                });
              }

              // Push 'must' conditions
              this.searchBuilder(search, must, e);
              return this.searchAllElastic({
                index,
                query: {
                  bool: {
                    must,
                    must_not,
                  },
                },
                size,
                sort_column,
                sort_direction,
              });
            }
          )
            .then((e) => {
              const normalized_data = uniqBy(
                flattenDeep(e.map((ee) => flattenDeep(ee.data))),
                "id"
              );
              return {
                count: normalized_data.length,
                data: normalized_data.map((res) => omit(res, "tombstone")),
              };
            })
            .catch((e) =>
              logger.error(`search_result: ${JSON.stringify(e, null, 2)}`)
            );

          return {
            entity: field_entity.replace("_v1", ""),
            search_result,
          };
        }
      )
        .filter((e) => e.search_result.count as unknown as boolean)
        .catch((_) => [] as any);

      const reducer = (
        accumulator: number,
        currentValue: { search_result: { count: number } }
      ): number => {
        return accumulator + currentValue.search_result.count;
      };
      return {
        count: entities_value.reduce(reducer, 0),
        data: entities_value,
        // data: {
        //   count: entities_value.length,
        //   entities: entities_value,
        // }
      };
    } catch (e: any) {
      logger.error(`[GlobalSearch][ERROR]: ${e}`);
    }
  }
  removeDatasetVersion(entity: string) {
    const split_entity = entity.split("_");
    return split_entity.splice(0, split_entity.length - 1).join("_");
  }
  async entitySearch(params: any) {
    const {
      company_name,
      search = "",
      entity,
      search_fields = [],
      pluck_fields = [],
      size = 50,
      start = 0,
      sort_column = "created_date",
      sort_direction = "desc",
      advanced_filter = [],
      is_or = true,
      schema_version = SCHEMA_VERSION,
    } = params;
    logger.info(
      `[EntitySearch] - [PARAMS]: ${JSON.stringify(params, null, 2)}`
    );
    const databases = await this.listDatabasesGlobalSearch(schema_version).then(
      (dbs) => {
        return dbs.filter((e) =>
          e
            .split("_")
            .join("")
            .includes(company_name.split(" ").join("").toLowerCase())
        );
      }
    );
    const entity_name = entity;
    const entities = await Bluebird.map(databases, async (database: string) => {
      return {
        index: `${database}-${entity_name}`,
        entity: entity_name,
        search_fields,
      };
    });

    const entities_value = await Bluebird.map(entities, async (entity: any) => {
      const { index, search_fields, entity: field_entity } = entity;
      const should: any = [];
      const must: any = [];
      const must_not: any = [
        {
          term: {
            status: "Deleted",
          },
        },
        {
          term: {
            tombstone: 1,
          },
        },
      ];

      if (advanced_filter.length) {
        advanced_filter.map((e: any) => {
          const { column, operator, value } = e;
          let arr = is_or ? should : must;
          if (operator === "deep_equal") {
            // Used for deep-equality (exact value) query
            must.push({
              terms: {
                [column]: Array.isArray(value) ? value : [value],
              },
            });
          } else if (operator === "not_equal") {
            // Opposite for deep-equality
            must_not.push({
              terms: {
                [column]: Array.isArray(value) ? value : [value],
              },
            });
          } else if (operator === "equal") {
            // NOTE: Equal operator however; equivalent behavior
            arr.push({
              wildcard: {
                [`${column}.edge_ngram`]: {
                  value: `${value}`,
                },
              },
            });
            // NOTE: uncomment this for operator equal (case sensitive) behavior
            // arr.push({
            //   terms: {
            //     [column]: Array.isArray(value) ? value : [value],
            //   },
            // });
          } else if (operator === "contains") {
            arr.push({
              wildcard: {
                [`${column}.edge_ngram`]: {
                  value: `*${value}*`,
                },
              },
            });
          } else if (operator === "does_not_contain") {
            must_not.push({
              query_string: {
                fields: [`${column}.edge_ngram`],
                query: `*${value}*`,
                default_operator: "AND",
              },
            } as any);
          } else if (operator === "starts_with") {
            arr.push({
              query_string: {
                fields: [`${column}.prefix`],
                query: `${value}`,
                default_operator: "AND",
              },
            });
          } else if (operator === "is_between") {
            arr.push({
              range: {
                [column]: {
                  gte: value[0],
                  lte: value[1],
                },
              },
            });
          } else if (operator === "is_not_between") {
            arr.push({
              bool: {
                must_not: [
                  {
                    range: {
                      [column]: {
                        gte: value[0],
                        lte: value[1],
                      },
                    },
                  },
                ],
              },
            });
          } else if (operator === "is_not_empty") {
            must_not.push({
              bool: {
                should: [
                  {
                    terms: {
                      [column]: [""],
                    },
                  },
                  {
                    script: {
                      script: {
                        source: `doc['${column}'].length == 0`,
                        lang: "painless",
                      },
                    },
                  },
                ],
              },
            });
          } else if (operator === "is_empty") {
            must.push({
              bool: {
                should: [
                  {
                    terms: {
                      [column]: [""],
                    },
                  },
                  {
                    script: {
                      script: {
                        source: `doc['${column}'].length == 0`,
                        lang: "painless",
                      },
                    },
                  },
                ],
              },
            });
          }
        });
        must.push({
          bool: {
            should,
          },
        });
      }

      if (search_fields.length) {
        const should: any = [];
        const builder = async (search_val = [], field: string) => {
          search_val = Array.isArray(search_val) ? search_val : [search_val];

          if (!search_val.length) return null;
          const data_should: any = [];
          search_val.forEach((value: string) => {
            value = value.trim().toLowerCase();
            if (value.includes(" ")) {
              data_should.push({
                multi_match: {
                  fields: [`${field}.edge_ngram`],
                  query: value,
                  type: "phrase_prefix",
                },
              });
            } else {
              data_should.push({
                wildcard: {
                  [`${field}.edge_ngram`]: {
                    value: `*${value}*`,
                  },
                },
              });
            }
          });

          return data_should;
        };
        await Bluebird.map(search_fields, async (e: string) => {
          const should_builder = await builder(search, e);
          should_builder.map((e: any) => {
            should.push(e);
          });
        });
        must.push({
          bool: {
            should,
          },
        });
      }

      const search_result = await this.elastic
        .search({
          index,
          body: {
            track_total_hits: true,
            query: {
              bool: {
                must,
                must_not,
                should,
              },
            },
            size,
            from: start,
            sort: [
              {
                [sort_column]: {
                  order: sort_direction,
                },
              },
            ],
            _source: pluck_fields,
          },
        })
        .then((e: any) => {
          const {
            hits: {
              total: { value },
              hits,
            },
          } = e;
          return {
            count: value,
            data: map(hits, (e) => ({ ...e._source })),
          };
        })
        .catch((e: any) =>
          logger.error(`entity_search_result: ${JSON.stringify(e, null, 2)}`)
        );

      return {
        entity: field_entity,
        search_result,
      };
    })
      .filter((e) => e.search_result.count as unknown as boolean)
      .catch((_) => [] as any);

    const reducer = (
      accumulator: number,
      currentValue: { search_result: { count: number } }
    ): number => {
      return accumulator + currentValue.search_result.count;
    };
    return {
      count: entities_value.reduce(reducer, 0),
      data: entities_value,
    };
  }

  async getVehicleRateByClass(params: any) {
    const {
      company_name = "",
      location_id = "",
      schema_version = SCHEMA_VERSION,
    } = params;
    logger.info(
      `[GetVehicleRateByClass] - [PARAMS]: ${JSON.stringify(params, null, 2)}`
    );
    const db_index_name = company_name.split(" ").join("").toLowerCase();
    const db = db_index_name + "_core_db_" + schema_version;
    const vehicles = await this.elastic
      .search({
        index: `${db}-vehicle`,
        body: {
          query: {
            bool: {
              must: [
                {
                  term: {
                    location_id,
                  },
                },
              ],
              must_not: [
                {
                  term: {
                    status: "Deleted",
                  },
                },
                {
                  term: {
                    tombstone: 1,
                  },
                },
              ],
            },
          },
          aggs: {
            class: {
              terms: {
                field: "class_name",
                size: 10,
              },
            },
          },
        },
      })
      .then(async (e: any) => {
        const {
          aggregations: {
            class: { buckets },
          },
        } = e;
        const vehicle_info = await Bluebird.map(buckets, async (ee: any) => {
          const { key, doc_count } = ee;
          const vehicle_ids = await this.searchAllElastic({
            index: `${db}-vehicle`,
            query: {
              bool: {
                must: [
                  {
                    term: {
                      class_name: key,
                    },
                  },
                ],
              },
            },
            fields: ["id"],
          }).then(({ data }) => data.map((e) => e.id));

          const vehicle_status_count = await this.elastic
            .search({
              index: `${db}-vehicle`,
              body: {
                query: {
                  bool: {
                    must: [
                      {
                        terms: {
                          id: vehicle_ids,
                        },
                      },
                    ],
                  },
                },
                aggs: {
                  vehicle_status: {
                    terms: {
                      field: "vehicle_status",
                      size: 10,
                    },
                  },
                },
              },
            })
            .then(
              ({
                aggregations: {
                  vehicle_status: { buckets = [] },
                },
              }) => {
                const data = buckets.reduce((acc: any, el: any) => {
                  return {
                    ...acc,
                    [el.key]: el.doc_count,
                  };
                }, {});
                return data;
              }
            );

          const min_max_rate = await this.elastic
            .search({
              index: `${db}-vehicle_rate`,
              body: {
                query: {
                  bool: {
                    must: [
                      {
                        terms: {
                          vehicle_id: vehicle_ids,
                        },
                      },
                    ],
                  },
                },
                aggs: {
                  min_rate: {
                    min: {
                      field: "minimum_rate",
                    },
                  },
                  max_rate: {
                    max: {
                      field: "maximum_rate",
                    },
                  },
                },
              },
            })
            .then(
              ({
                aggregations: {
                  min_rate = { value: 0 },
                  max_rate = { value: 0 },
                },
              }) => {
                return {
                  rates: {
                    min_rate: min_rate.value,
                    max_rate: max_rate.value,
                  },
                };
              }
            );

          const default_statuses = {
            available: 0,
            not_so_available: 0,
            unavailable: 0,
          };

          return {
            class: key,
            vehicle_count: doc_count,
            vehicle_status_count: {
              ...default_statuses,
              ...vehicle_status_count,
            },
            ...min_max_rate,
          };
        });

        return vehicle_info;
      });
    return vehicles;
  }

  async getByMultipleFilter(params: any) {
    const {
      database = "core_db",
      entity = "calendar_event",
      filters = [],
      ranges = [],
      size = 50,
      sort_column = "created_date",
      sort_direction = "desc",
      schema_version = SCHEMA_VERSION,
    } = params;
    logger.info(
      `[GetByMultipleFilter] - [PARAMS]: ${JSON.stringify(params, null, 2)}`
    );
    const index = `${database}_${schema_version}-${entity}`;
    const must: any = [];
    const must_not: any = [
      {
        term: {
          status: "Deleted",
        },
      },
      {
        term: {
          tombstone: 1,
        },
      },
    ];

    if (ranges.length) {
      await Bluebird.map(ranges, (e: any) => {
        const { key, value, operation } = e;
        must.push({
          range: {
            [`${key}`]: {
              [`${operation}`]: value,
            },
          },
        });
      });
    }

    if (filters.length) {
      await Bluebird.map(filters, (e: any) => {
        const { key, value, values } = e;
        const new_value = value.length ? [value] : values;

        must.push({
          terms: {
            [`${key}`]: flattenDeep(new_value),
          },
        });
      });
    }

    const result = await this.searchAllElastic({
      index,
      query: {
        bool: {
          must,
          must_not,
        },
      },
      size,
      sort_column,
      sort_direction,
    }).then(({ data, count: { value } }) => {
      return {
        count: value,
        data: data.map((e) => omit(e, "tombstone")),
      };
    });

    return {
      count: result.count,
      data: result.data,
    };
  }

  async getNearbyLocation(params: any) {
    /**
     * Sample request body value:
     *
     *    pickup_location: { lat, lng }
     *    dropoff_location: { lat, lng }
     *    circle_radius: 10000 in meters
     *
     *
     * Goal:
     *    Get all nearby locations from pickup_location
     *    Get all nearby locations from dropoff_location
     *    - Where pickup and dropoff location are the center of the circle
     */
    const {
      database = "", // = 'gorentals_core_db_v26',
      entity = "", // location,
      pickup_location,
      dropoff_location,
      // vehicle_make_model_id,
      circle_radius,
    } = params;
    logger.info(
      `[GetNearbyLocation] - [PARAMS]: ${JSON.stringify(params, null, 2)}`
    );

    const pickup_location_center = {
      lat: pickup_location.lat,
      lon: pickup_location.lng,
    };
    const dropoff_location_center = {
      lat: dropoff_location.lat,
      lon: dropoff_location.lng,
    };

    const must_not = [
      {
        term: {
          status: "Deleted",
        },
      },
      {
        term: {
          tombstone: 1,
        },
      },
      {
        terms: {
          geolocation_lat: [
            pickup_location_center.lat,
            dropoff_location_center.lat,
          ],
        },
      },
      {
        terms: {
          geolocation_lng: [
            pickup_location_center.lon,
            dropoff_location_center.lon,
          ],
        },
      },
    ];

    const nearby_locations = await this.searchAllElastic({
      index: `${database}-${entity}`,
      query: {
        bool: {
          must_not,
        },
      },
      size: 50,
    }).then(async ({ data }) => {
      const unique_data = uniqWith(
        data,
        (loc_a, loc_b) =>
          loc_a.geolocation_lat === loc_b.geolocation_lat &&
          loc_a.geolocation_lng === loc_b.geolocation_lng
      );

      const nearby_location = await Bluebird.filter(
        unique_data,
        async (e: any) => {
          const { geolocation_lat, geolocation_lng } = e;
          const geolocation = { lat: geolocation_lat, lon: geolocation_lng };
          const test_result =
            (await insideCircle(
              geolocation,
              pickup_location_center,
              circle_radius
            )) ||
            (await insideCircle(
              geolocation,
              dropoff_location_center,
              circle_radius
            ));
          return test_result;
        }
      ).then((e) => e.map((ee) => omit(ee, "tombstone")));
      return {
        count: nearby_location.length,
        data: nearby_location,
      };
    });
    return nearby_locations;
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

export default ElasticDbStore;
