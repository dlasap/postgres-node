import Transport from "../Transport";
import { ConnectOptions } from "rethinkdbdash";
import { ClientConfig } from "pg";
import MessageStore from "../MessageStore";
import { Config as MeilisearchConfig } from "meilisearch";
// import { type } from 'node:os';

export type Operation = () => Promise<void>;

export type OperationTable = {
  table_create: Operation;
  database_create: Operation;
  index_create: Operation;
  update: Operation;
};

export interface IStatuses {
  is_active: boolean;
  is_inactive: boolean;
  is_archived: boolean;
  is_draft: boolean;
}

export type OperationType = keyof OperationTable;

export interface Message<TPayload = any> {
  id: string;
  sender_id?: string;
  transaction_id: string;
  system_id?: string;
  service_id: string;
  operation: OperationType;
  database: string;
  dataset: string;
  row: string;
  column: string;
  value: TPayload;
  timestamp: string;
  entity_fields: Record<any, any>;
  options?: Record<string, any>;
}

export interface StoreConfig {
  initial_state?: any;
  transport: Transport;
  message_store: MessageStore;
}
export interface RedisConfig {
  port: number;
  host: string;
  password?: string;
  db: number;
}
export interface RedisStoreConfig extends StoreConfig {
  config: RedisConfig;
}

export interface RethinkdbStoreConfig extends StoreConfig {
  config: ConnectOptions;
  transport: Transport;
}

export interface PostgresStoreConfig extends StoreConfig {
  config: ClientConfig;
  transport: Transport;
}
export interface ElasticConfig {
  host: string;
  requestTimeout: number;
  httpAuth: string;
}
export interface ElasticdbStoreConfig extends StoreConfig {
  config: ElasticConfig;
  transport: Transport;
}
export interface MeilisearchdbStoreConfig extends StoreConfig {
  config: MeilisearchConfig;
  transport: Transport;
}

export type StoreLookupOptions<TIndexValuePair> = {
  [Property in keyof TIndexValuePair as string]: string;
};

export interface StoreOrderOptions {
  order_directions?: "ASCENDING" | "DESCENDING";
  order_key?: string;
  start: number;
  limit: number;
}

export interface IFilterCriteria {
  field: string;
  operation:
    | "equal"
    | "contains"
    | "starts_with"
    | "is_between"
    | "not_equal"
    | "does_not_contains"
    | "less_than"
    | "greater_than";
  attribute?: boolean;
  values: Array<string>;
}
export type TBasicType = string | number | boolean;
export interface ISingleFilterCriteriaRequest
  extends StoreAccessCollectionParameters {
  filter: IFilterCriteria;
}

export interface ISearchParams extends StoreAccessCollectionParameters {
  search: string;
}

export interface StoreAccessBaseParameters {
  database: string;
  dataset: string;
}

// Access Single Items
export interface StoreAccessItemParameters extends StoreAccessBaseParameters {
  id: string;
}

// Access Collection of Items
export interface StoreAccessCollectionParameters
  extends StoreAccessBaseParameters {
  order: StoreOrderOptions;
}

// Access Collection by Index
export interface StoreAccessByIndexParameters<TIndexValuePair>
  extends StoreAccessCollectionParameters {
  indexes: StoreLookupOptions<TIndexValuePair>;
}
export interface AdvancedFilterParameters {
  operation: string;
  column: string;
  value: string | [string];
}

// Access By Filter
export interface StoreAccessByFilterParameters<TFilterValuePair>
  extends StoreAccessCollectionParameters {
  filters: StoreLookupOptions<TFilterValuePair>;
  advanced_filter?: [AdvancedFilterParameters];
  is_or?: boolean;
}

export interface StoreItem {
  id: string;
  tombstone: 0 | 1;
  attribute: Record<string, any>;
  [key: string]: any;
}

export interface SystemQueryOperations {
  listDatabases: () => Promise<string[]>;
  listTables: (database: string) => Promise<string[]>;
  listIndex: (database: string, dataset: string) => Promise<string[]>;
}

export interface SystemTransactionsOperations {
  createDatabase: (database: string) => Promise<void>;
  createTable: (database: string, dataset: string) => Promise<void>;
  createIndex: (
    database: string,
    dataset: string,
    index: string
  ) => Promise<void>;
}

export interface SystemUpdateOperations {
  applyCreateDatabase: (database: string, options?: any) => Promise<void>;
  applyCreateTable: (
    database: string,
    dataset: string,
    options?: any
  ) => Promise<void>;
  applyCreateIndex: (
    database: string,
    dataset: string,
    column: string,
    options?: any
  ) => Promise<void>;
  applyUpdate: (message: Message) => Promise<void>;
}

export interface StoreAccessOperations<TRowSchema = any> {
  getById: (params: StoreAccessItemParameters) => Promise<StoreItem | null>;
  getByIndex: (
    params: StoreAccessByIndexParameters<TRowSchema>
  ) => Promise<StoreItem[]>;
  getByFilter: (
    params: StoreAccessByFilterParameters<TRowSchema>
  ) => Promise<StoreItem[]>;
  list: (params: StoreAccessCollectionParameters) => Promise<StoreItem[]>;
  getBySingleFilter: (
    params: ISingleFilterCriteriaRequest
  ) => Promise<StoreItem[]>;
  search: (params: ISearchParams) => Promise<StoreItem[]>;
}

export interface StoreTransactionOperations {
  insert: (database: string, dataset: string, row: any) => Promise<string>;
  update: (database: string, dataset: string, row: any) => Promise<string>;
  delete: (database: string, dataset: string, row: any) => Promise<string>;
}

export interface MessageStoreOperations {
  initialize: () => Promise<void>;
  add: (message: Message) => Promise<void>;
  getMessages: () => Promise<Message[]>;
  compare: (messages: Message[]) => Promise<Map<any, any>>;
}

export interface ElasticSeachOperations {
  globalSearch: (params: any) => Promise<any>;
}

interface IElasticSeachDbTables {
  id: string;
  name: string;
}
export interface IElasticSearchInsertIndexParams {
  id?: string;
  name?: string;
  tables?: IElasticSeachDbTables[];
  system_id?: string;
}

export interface IStoredElasticSearchTAbles {
  database: string;
  table: string;
}
export interface IElasticSearchSetting {
  number_of_shards: number;
  number_of_replicas: number;
  mapping: {
    total_fields: {
      limit: number;
    };
  };
  analysis: {
    normalizer: {
      sortable: {
        type: string;
        filter: string[];
      };
    };
    analyzer: {
      // english: {
      //   type: string;
      //   tokenizer: string;
      //   filter: string[];
      // };
      edge_ngram: {
        tokenizer: string;
        filter: string[];
      };
    };
    tokenizer: {
      edge_ngram: {
        type: string;
        min_gram: number;
        max_gram: number;
      };
    };
  };
}

export interface IGetCode {
  entity_fields?: any;
  dataset?: string;
  system_id: string;
  column: string;
  value: string;
  system_code: any;
}
export interface ISystemCode {
  prefix_code: string;
  number_scheme: "incremental" | "random" | "";
  suffix_code: string[];
  default_code_number: number;
  name: string;
}

export interface IElasticSearchMapping {
  dynamic_templates: [
    {
      empty_date: {
        match: string;
        mapping: {
          type: string;
        };
      };
    },
    // ! deprecated
    // {
    //   date: {
    //     match_mapping_type: string;
    //     mapping: {
    //       type: string;
    //     };
    //   };
    // },
    {
      integer: {
        match_mapping_type: string;
        mapping: {
          type: string;
          fields: {
            edge_ngram: {
              type: string;
              analyzer: string;
              search_analyzer: string;
            };
          };
        };
      };
    },
    {
      double: {
        match_mapping_type: string;
        mapping: {
          type: string;
          fields: {
            edge_ngram: {
              type: string;
              analyzer: string;
              search_analyzer: string;
            };
          };
        };
      };
    },
    {
      boolean: {
        match_mapping_type: string;
        mapping: {
          type: string;
          fields: {
            edge_ngram: {
              type: string;
              analyzer: string;
              search_analyzer: string;
            };
          };
        };
      };
    },
    {
      object: {
        match_mapping_type: string;
        mapping: {
          ignore_malformed: boolean;
        };
      };
    },
    {
      strings: {
        match_mapping_type: string;
        unmatch: string;
        mapping: {
          type: string;
          fields: {
            // english: {
            //   type: string;
            //   analyzer: string;
            // };
            edge_ngram: {
              type: string;
              analyzer: string;
              search_analyzer: string;
            };
            sort: {
              type: string;
              normalizer: string;
              ignore_above: number;
            };
          };
        };
      };
    }
  ];
}

type TElasticSearchTerminology =
  | {
      term?: {
        [key: string]: any;
      };
    }
  | {
      terms?: {
        [key: string]: any[];
      };
    };

export interface IElasticSearchBody extends IElasticSearchInsertIndexParams {
  query?: IElasticSearchQuery;
  size?: number;
  index?: string;
  type?: string;
}
export interface IElasticSearchQuery {
  bool: {
    must?: TElasticSearchTerminology | TElasticSearchTerminology[];
    must_not?: TElasticSearchTerminology | TElasticSearchTerminology[];
    filter?: TElasticSearchTerminology | TElasticSearchTerminology[];
    should?: TElasticSearchTerminology | TElasticSearchTerminology[];
    should_not?: TElasticSearchTerminology | TElasticSearchTerminology[];
  };
}

export interface IPaginationParams {
  start: number;
  limit: number;
}
export interface IMigrationOperations {
  count: (params: StoreAccessBaseParameters) => Promise<any>;
  paginatedResults: (
    params: StoreAccessByFilterParameters<any>
  ) => Promise<any[]>;
}
