import { StoreItem, IFilterCriteria } from "../../types";
import bluebird from "bluebird";

const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
  threadId = 1,
} = require("worker_threads");

const matchSearchTexts = async (
  record_strings: Array<string>,
  search_term: string
): Promise<Array<StoreItem>> => {
  return await bluebird.reduce<string, StoreItem[]>(
    record_strings,
    (store_items, raw_store_item) => {
      const evaluation =
        raw_store_item.toLowerCase().includes(search_term.toLowerCase()) &&
        raw_store_item.includes(`"tombstone":0`);

      if (evaluation) return [...store_items, JSON.parse(raw_store_item)];

      return store_items;
    },
    []
  );
};

if (isMainThread) {
  module.exports = function matchAnyReducerAsync(
    records: Array<StoreItem>,
    search_term: string
  ): Promise<Array<StoreItem>> {
    return new Promise((resolve, reject) => {
      const worker = new Worker(__filename, {
        workerData: {
          records,
          search_term,
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
  const { records, search_term } = workerData;
  matchSearchTexts(records, search_term).then((data) => {
    parentPort.postMessage(data);
  });
}
