import {StoreItem, IFilterCriteria, TBasicType} from "../../types";
import bluebird from "bluebird";

const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} = require("worker_threads");

const matchSingleFilter = async (
  records: Array<StoreItem>,
  filter: IFilterCriteria
): Promise<Array<StoreItem>> => {
  const data = await bluebird.filter(records, (record: StoreItem) => {
    const {attribute, tombstone} = record;
    const {field, operation, values, attribute: in_attribute} = filter;

    if (tombstone) return false;

    if (!record[field] && !attribute[field]) return false;

    const resolved_value = in_attribute ? attribute[field] : record[field];
    if (!resolved_value) return false;

    const contains = (search_values: TBasicType[], resolved_value: any) => {
      return search_values.reduce((acc, filter_value) => {
        if (typeof filter_value !== "string") {
          return acc || false;
        }
        const resolved_string =
          typeof resolved_value !== "string"
            ? JSON.stringify(resolved_value)
            : resolved_value;
        return (
          acc ||
          resolved_string.toLowerCase().includes(filter_value.toLowerCase())
        );
      }, false) as boolean;
    };

    const equal = (search_values: TBasicType[], resolved_value: any) => {
      return search_values.reduce((acc, filter_value: any) => {
        if (
          typeof resolved_value === "string" &&
          typeof filter_value === "string"
        ) {
          return (
            acc || resolved_value.toLowerCase() === filter_value.toLowerCase()
          );
        }
        return acc || resolved_value === filter_value;
      }, false) as boolean;
    };

    switch (operation) {
      case "contains":
        return contains(values, resolved_value);
      case "does_not_contains":
        return !contains(values, resolved_value);
      case "equal":
        return equal(values, resolved_value);
      case "not_equal":
        return !equal(values, resolved_value);
      case "less_than":
        const is_less_than = values.reduce((acc, filter_value) => {
          if (
            typeof filter_value !== "number" ||
            typeof resolved_value !== "number"
          ) {
            return acc || false;
          }
          return acc || filter_value < resolved_value;
        }, false);
        return is_less_than as boolean;
      case "greater_than":
        const is_greater_than = values.reduce((acc, filter_value) => {
          if (
            typeof filter_value !== "number" ||
            typeof resolved_value !== "number"
          ) {
            return acc || false;
          }
          return acc || filter_value > resolved_value;
        }, false);
        return is_greater_than;
      case "starts_with":
        const is_starts_with_match = values.reduce((acc, filter_value) => {
          if (
            typeof resolved_value !== "string" ||
            typeof filter_value !== "string"
          ) {
            return acc || false;
          }
          return (
            acc ||
            resolved_value.toLowerCase().startsWith(filter_value.toLowerCase())
          );
        }, false);
        return is_starts_with_match;
      case "is_between":
        const is_between_match = values.reduce((acc, filter_value) => {
          if (
            typeof resolved_value !== "string" ||
            typeof filter_value !== "string"
          ) {
            return acc || false;
          }
          const [start, end] = filter_value.split(",");

          if (
            !start ||
            !end ||
            isNaN(parseInt(start)) ||
            isNaN(parseInt(end)) ||
            isNaN(parseInt(resolved_value))
          ) {
            return acc || false;
          }

          const resolved_start = parseInt(start);
          const resolved_end = parseInt(end);
          const resolved_number = parseInt(resolved_value);
          const is_between_item_match =
            resolved_number >= resolved_start &&
            resolved_number <= resolved_end;

          return acc || is_between_item_match;
        }, false);

        return is_between_match;

      default:
        return false;
    }
  });

  return data;
};

if (isMainThread) {
  module.exports = function matchAnyReducerAsync(
    records: Array<StoreItem>,
    filter: IFilterCriteria
  ): Promise<Array<StoreItem>> {
    return new Promise((resolve, reject) => {
      const worker = new Worker(__filename, {
        workerData: {
          records,
          filter,
        },
      });
      worker.on("message", resolve);
      worker.on("error", reject);
      worker.on("exit", (code: any) => {
        if (code !== 0)
          reject(new Error(`Worker stopped with exit code ${code}`));
      });
    });
  };
} else {
  const {records, filter} = workerData;

  matchSingleFilter(records, filter).then((data) =>
    parentPort.postMessage(data)
  );
}
