import { IElasticSearchSetting } from "../../types";

const settings: IElasticSearchSetting = {
  number_of_shards: 6,
  number_of_replicas: 1,
  mapping: {
    total_fields: {
      limit: 10000,
    },
  },
  analysis: {
    normalizer: {
      sortable: {
        type: "custom",
        filter: ["lowercase"],
      },
    },
    analyzer: {
      // english: {
      //   type: "text",
      //   tokenizer: 'standard',
      //   filter: ['lowercase'],
      // },
      edge_ngram: {
        tokenizer: "edge_ngram",
        filter: ["lowercase"],
      },
    },
    tokenizer: {
      edge_ngram: {
        type: "edge_ngram",
        min_gram: 1,
        max_gram: 32,
      },
    },
  },
};

export default settings;
