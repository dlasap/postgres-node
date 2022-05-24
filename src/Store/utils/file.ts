import fs_promise from "fs/promises";
import fs from "fs";
import path from "path";
import Redis from "ioredis";
import rethinkdbdash, { ConnectOptions, Client } from "rethinkdbdash";

const {
  SAVE_SYSTEM_ID_TO = "file",
  SYSTEM_ID_REDIS_HOST,
  SYSTEM_ID_REDIS_PORT,
  SYSTEM_ID_REDIS_DB,
  SYSTEM_ID_RETHINK_SERVERS = "localhost:28015,", // dom: 10.100.100.159:8082 :8085 // qa-server: http://216.34.250.110:28015 :8080/
  SYSTEM_ID_RETHINK_PASSWORD,
  SYSTEM_ID_RETHINK_DB = "gorentals",
} = process.env;

let redis_client: any = null;
let rethink_client: Client;

if (SAVE_SYSTEM_ID_TO === "redis_db") {
  redis_client = new Redis({
    port: Number(SYSTEM_ID_REDIS_PORT),
    host: SYSTEM_ID_REDIS_HOST,
    db: Number(SYSTEM_ID_REDIS_DB),
  } as Redis.RedisOptions);

  redis_client.on("connect", () => {
    console.log(
      `Redis for system id is Connected to ${SYSTEM_ID_REDIS_HOST}:${SYSTEM_ID_REDIS_PORT}`
    );
  });
} else if (SAVE_SYSTEM_ID_TO === "rethink_db") {
  let list_of_servers = SYSTEM_ID_RETHINK_SERVERS.split(",")
    .filter(Boolean)
    .map((e: any) => {
      const server = e.split(":");
      return {
        host: server[0],
        port: server[1],
      };
    });

  rethink_client = rethinkdbdash({
    servers: list_of_servers as any[],
    // password: SYSTEM_ID_RETHINK_PASSWORD,
    // db: SYSTEM_ID_RETHINK_DB,
  } as ConnectOptions);
}
const id = "ab165c4b-2289-4d85-bc95-4dcd44c3e6a3";
export const saveSystemId = async (
  save_to: string,
  company_name: string,
  dataset: string,
  current_data: Record<string, any>,
  prev_data: Record<string, any>
) => {
  const data = { ...prev_data, ...current_data };
  const filename = `${path.resolve(
    process.cwd()
  )}/${company_name}_system_id.json`;
  switch (save_to) {
    case "redis_db":
      const key = `${company_name}:${dataset}`;
      const row = (await redis_client?.get(key)) as string;
      await redis_client?.set(
        key,
        JSON.stringify({
          ...(row ? JSON.parse(row) : { system_id: "0" }),
          system_id: data[dataset].system_id,
        })
      );
      break;
    case "rethink_db":
      const table = `${dataset}`;
      await rethink_client
        .dbList()
        .contains(company_name)
        .do(function (databaseExists) {
          return rethink_client.branch(
            databaseExists,
            // @ts-ignore
            { dbs_created: 1 },
            rethink_client.dbCreate(company_name)
          );
        })
        .run();
      await rethink_client
        .db(company_name)
        .tableList()
        .contains(dataset)
        .do(function (tableExists) {
          return rethink_client.branch(
            tableExists,
            // @ts-ignore
            { table_created: 1 },
            rethink_client.db(company_name).tableCreate(dataset)
          );
        })
        .run();

      if (!Number(prev_data[dataset]?.system_id))
        await rethink_client
          .db(company_name)
          .table(table)
          .insert(data[dataset], { conflict: "replace" })
          .run();

      await rethink_client
        .db(company_name)
        .table(table)
        .get(id)
        .update(data[dataset])
        .run();
      break;
    default:
      console.log(
        `Creating/Updating file [${company_name}_system_id.json] in ${filename}, [SYSTEM_ID]: ${data[dataset].system_id}`
      );
      fs.writeFileSync(filename, JSON.stringify(data, null, 2), "utf-8");
      break;
  }
};

export const getSystemId = async (params: {
  get_to: string;
  company_name: string;
  dataset: string;
  current_record_id: string;
  row_id: string;
}): Promise<any> => {
  const { get_to, company_name, dataset, current_record_id, row_id } = params;
  const filename = `${path.resolve(
    process.cwd()
  )}/${company_name}_system_id.json`;
  let current_system_id = "0";
  switch (get_to) {
    case "redis_db":
      const key = `${company_name}:${dataset}`;
      try {
        await redis_client?.incr(key);
      } catch (e) {
        console.error("[PARSE ERROR]: fixing value", e);
        const prev_data = await redis_client?.get(key);
        const parsed_data = JSON.parse(prev_data);
        await redis_client?.set(key, Number(parsed_data?.system_id) + 1 ?? 0);
      }
      return redis_client?.get(key);
    case "rethink_db":
      try {
        const table = `${dataset}`;
        const results: any = await rethink_client
          .db(company_name)
          .table(table)
          .get(id)
          .run();
        return JSON.stringify(results);
      } catch (e) {
        console.error(`[ERROR]: ${JSON.stringify(e)}`);
        return JSON.stringify({ id, system_id: current_system_id });
      }
    default:
      const data: any = await fs_promise.readFile(filename, "utf8");
      return Buffer?.from(data)?.toString() || JSON.stringify({});
  }
};
