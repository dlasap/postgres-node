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
    {
      date: {
        match_mapping_type: string;
        mapping: {
          type: string;
        };
      };
    },
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
        [key: string]: string;
      };
    }
  | {
      terms?: {
        [key: string]: string[];
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
    must?: TElasticSearchTerminology;
    must_not?: TElasticSearchTerminology;
    filter?: TElasticSearchTerminology;
    should?: TElasticSearchTerminology;
    should_not?: TElasticSearchTerminology;
  };
}
