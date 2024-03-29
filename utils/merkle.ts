import { Timestamp } from "../models/Core/Timestamp";

/*
  Filename: merkle.ts
  Description:  Typescript port from merkle.js by James Long
*/

export function getKeys(trie: any) {
  return Object.keys(trie).filter((x) => x !== "hash");
}

export function keyToTimestamp(key: string) {
  // 16 is the length of the base 3 value of the current time in
  // minutes. Ensure it's padded to create the full value
  let fullkey = key + "0".repeat(16 - key.length);

  // Parse the base 3 representation
  return parseInt(fullkey, 3) * 1000 * 60;
}

export function insert(trie: any, timestamp: Timestamp) {
  let hash = timestamp.hash();
  let key = Number((timestamp.millis() / 1000 / 60) | 0).toString(3);

  trie = Object.assign({}, trie, { hash: trie.hash ^ hash });
  return insertKey(trie, key, hash);
}

export function insertKey(trie: any, key: string, hash: number): any {
  if (key.length === 0) {
    return trie;
  }
  const c = key[0];
  const n = trie[c] || {};
  return Object.assign({}, trie, {
    [c]: Object.assign({}, n, insertKey(n, key.slice(1), hash), {
      hash: n.hash ^ hash,
    }),
  });
}

export function build(timestamps: Timestamp[]) {
  let trie = {};
  for (let timestamp of timestamps) {
    insert(trie, timestamp);
  }
  return trie;
}

export function diff(trie1: any, trie2: any) {
  if (trie1.hash === trie2.hash) {
    return null;
  }

  let node1 = trie1;
  let node2 = trie2;
  let k = "";

  while (1) {
    let keyset = new Set([...getKeys(node1), ...getKeys(node2)]);
    let keys = [...keyset.values()];
    keys.sort();

    let diffkey = keys.find((key) => {
      let next1 = node1[key] || {};
      let next2 = node2[key] || {};
      return next1.hash !== next2.hash;
    });

    if (!diffkey) {
      return keyToTimestamp(k);
    }

    k += diffkey;
    node1 = node1[diffkey] || {};
    node2 = node2[diffkey] || {};
  }
}

export function prune(trie: any, n = 2) {
  // Do nothing if empty
  if (!trie.hash) {
    return trie;
  }

  let keys = getKeys(trie);
  keys.sort();

  let next = { hash: trie.hash };
  //@ts-ignore - Rebuild Calculation
  keys = keys.slice(-n).map((k) => (next[k] = prune(trie[k], n)));

  return next;
}

export function debug(trie: any, k = "", indent = 0): string {
  const str =
    " ".repeat(indent) +
    (k !== "" ? `k: ${k} ` : "") +
    `hash: ${trie.hash || "(empty)"}\n`;
  return (
    str +
    getKeys(trie)
      .map((key) => {
        return debug(trie[key], key, indent + 2);
      })
      .join("")
  );
}
