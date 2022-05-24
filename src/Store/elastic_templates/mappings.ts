import { IElasticSearchMapping } from "../../types";

const mappings: IElasticSearchMapping = {
  dynamic_templates: [
    {
      empty_date: {
        match: "*_date",
        mapping: {
          type: "double",
        },
      },
    },
    {
      integer: {
        match_mapping_type: "long",
        mapping: {
          type: "double",
          fields: {
            edge_ngram: {
              type: "text",
              analyzer: "edge_ngram",
              search_analyzer: "edge_ngram",
            },
          },
        },
      },
    },
    {
      double: {
        match_mapping_type: "double",
        mapping: {
          type: "double",
          fields: {
            edge_ngram: {
              type: "text",
              analyzer: "edge_ngram",
              search_analyzer: "edge_ngram",
            },
          },
        },
      },
    },
    {
      boolean: {
        match_mapping_type: "boolean",
        mapping: {
          type: "boolean",
          fields: {
            edge_ngram: {
              type: "text",
              analyzer: "edge_ngram",
              search_analyzer: "edge_ngram",
            },
          },
        },
      },
    },
    {
      object: {
        match_mapping_type: "object",
        mapping: {
          ignore_malformed: true,
        },
      },
    },
    {
      strings: {
        match_mapping_type: "string",
        unmatch: "*_date",
        mapping: {
          type: "keyword",
          fields: {
            // english: {
            //   type:"text",
            //   analyzer:"english"
            // },
            edge_ngram: {
              type: "text",
              analyzer: "edge_ngram",
              search_analyzer: "edge_ngram",
            },
            sort: {
              type: "keyword",
              normalizer: "sortable",
              ignore_above: 256,
            },
          },
        },
      },
    },
  ],
};

export default mappings;
