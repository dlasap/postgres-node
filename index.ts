require("dotenv").config();
import { v4 } from "uuid";
import { Client, Pool, PoolConfig, ClientConfig } from "pg";
const {
  PGUSER = "admin",
  PGHOST = "localhost",
  PGPASSWORD = "admin",
  PGDATABASE = "core_db_v1",
  PGPORT = "5434",
} = process.env;

const pg_settings: ClientConfig = {
  user: PGUSER,
  password: PGPASSWORD,
  host: PGHOST,
  port: Number(PGPORT),
  database: PGDATABASE,
};

const executeClient = async () => {
  try {
    const client = new Client(pg_settings);
    await client.connect();
    client.query("SELECT NOW()", (error, _) => {
      error ? console.log("POOL ERROR :", error) : "";
    });
    return client;
  } catch (error: any) {
    console.log("Client Error", error);
    throw new Error(error);
  }
};

const createDatabase = async (client: Client, db_name: string) => {
  try {
    return await client.query(`CREATE DATABASE ${db_name}`);
  } catch (error: any) {
    console.log("Create Database Error: ", error.message);
  }
};

const createPool = async (db_name: string) => {
  try {
    const pool = new Pool({
      user: PGUSER,
      host: PGHOST,
      database: db_name,
      password: PGPASSWORD,
      port: Number(PGPORT),
      idleTimeoutMillis: 0,
      connectionTimeoutMillis: 0,
    } as PoolConfig);

    pool.query("SELECT NOW()", (error, _) => {
      error ? console.log("POOL ERROR :", error) : "";
    });
    return pool;
  } catch (error) {
    console.log("Pool error:", error);
  }
};
const listDatabases = async (client: Client) => {
  return await client
    .query(
      `
    SELECT datname FROM pg_database
    WHERE datistemplate = false;
    `
    )
    .then(({ rows }) => rows.map(({ datname }) => datname))
    .catch((err) => console.log("List Database Error: ", err.stack));
};

const createTable = async (
  client: Client,
  table_name: string,
  options?: any
) => {
  try {
    const query = `
        CREATE TABLE IF NOT EXISTS "${table_name}"
        (
        id UUID PRIMARY KEY,
        role_id UUID NOT NULL, 
        role_name text, 
        name text NOT NULL,
        image_urls text[] NOT NULL, 
        attribute json NOT NULL,
        FOREIGN KEY (role_id) REFERENCES roles(id),
        created_date BIGINT NOT NULL,
        updated_date BIGINT
        );
        `;
    const result = await client.query(query);
    return result;
  } catch (error: any) {
    throw new Error(`ERROR CREATE TABLE: ${error}`);
  }
};
const listTables = async (client: Client, db_name: string) => {
  try {
    const text = `
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog' AND 
        schemaname != 'information_schema';
        `;
    const tables = await client
      .query(text)
      .then(({ rows }) =>
        rows.map(({ tablename }) => tablename).filter(Boolean)
      )
      .catch((err) => console.log("List Database Error: ", err.stack));
    return tables;
  } catch (err: any) {
    console.log(err.stack);
  }
};

const insertPostgres = async (
  client: Client,
  table_name: string,
  data: any
) => {
  try {
    const fields = Object.keys(data).toString();
    const values = Object.values(data);
    const value_str = Object.keys(data)
      .map((_, index) => `$${index + 1}`)
      .toString();
    const query = {
      text: `INSERT INTO "${table_name}"(${fields}) VALUES(${value_str})`,
      values,
    };
    return await client.query(query);
  } catch (error: any) {
    throw new Error(`INSERT ERROR : ${error}`);
  }
};

const getPostgresUpdateQuery = (payload: Record<string, any>) => {
  let set_query = "";
  let values: Array<any> = [];
  let num = 0;

  Object.entries(payload).forEach(([key, value], index) => {
    if (key === "id") return;
    num += 1;
    set_query += `${key}=\$${num},`;
    values.push(value);
  });

  return {
    set_query: set_query.slice(0, -1),
    values,
  };
};

const getPostgresInsertQuery = (payload: any) => {
  const fields = Object.keys(payload);
  let count = 0;
  return {
    fields,
    set_values: fields
      .map((_) => {
        count++;
        return `$${count}`;
      })
      .toString(),
    values: Object.values(payload),
  };
};

const createPostgresTableQuery = (options: any) => {
  const { fields, primary_keys } = options;
  let formed_field_types: Array<string> = [];
  fields.forEach(({ field_name, data_type }: any) => {
    formed_field_types.push(
      `${field_name} ${data_type} ${
        primary_keys.includes(field_name) ? "PRIMARY KEY" : ""
      }`.trim()
    );
  });
  return formed_field_types.toString();
};

const main = async () => {
  try {
    const client = await executeClient();
    await createDatabase(client, "core_db_v1");
    const pool = await createPool("core_db_v1");
    const tables = await listTables(client, "core_db_v1");
    console.log(
      "%c 🌽 tables: ",
      "font-size:20px;background-color: #FFDD4D;color:#fff;",
      tables
    );
    const databases = await listDatabases(client);
    console.log(
      "%c 🥝 databases: ",
      "font-size:20px;background-color: #E41A6A;color:#fff;",
      databases
    );
    // await client.query()
    const pool_query = await client?.query(
      'SELECT * FROM "roles" WHERE id=$1',
      ["9b083f35-f96e-4286-9989-250c19625cd2"]
    );
    console.log(
      "%c 🍋 pool_query: ",
      "font-size:20px;background-color: #EA7E5C;color:#fff;",
      pool_query.rows
    );
    // const create_tables = await createTable(client, "core_db_v1-user")
    const {
      rows: [result],
    } = await client?.query('SELECT * FROM "roles" WHERE id=$1', [
      "9b083f35-f96e-4286-9989-250c19625cd1",
    ]);

    const { rows: query_result } = await client.query(
      'SELECT * FROM "gorentals_core_db_v4-vehicle";'
    );
    console.log(
      "%c 🥝 query_result: ",
      "font-size:20px;background-color: #EA7E5C;color:#fff;",
      query_result[0].files
    );

    const data_for_update = {
      //   name: "Extra Admin",
      updated_date: 1651039924826,
      entity: "hehe",
      id: "364424f5-50a3-4c46-a279-66fb5fb05245",
      tombstone: 1,
    };
    // const data_for_insert = {
    //   name: "X Admin",
    //   created_date: 1651039924876,
    //   id: "1a255705-3063-49f1-a000-fc1148664044",
    //   role_id: "9b083f35-f96e-4286-9989-250c19625cd0",
    //   image_urls: ["z.com"],
    //   attribute: {
    //     name: data_for_update.name,
    //   },
    // };

    const { set_query, values } = getPostgresUpdateQuery(data_for_update);
    // const {
    //   fields,
    //   values: insert_values,
    //   set_values,
    // } = getPostgresInsertQuery(data_for_insert);

    const update_query = `
        UPDATE "core_db_v1-syc_con"
        SET ${set_query}
        WHERE id=$${values.length + 1};
        `;
    // const insert_query = `
    //     INSERT INTO "core_db_v1-user"
    //     (${fields.toString()})
    //     VALUES (${set_values});
    //     `;

    const update_result = await client.query(update_query, [
      ...values,
      data_for_update.id,
    ]);
    // const insert_result = await client.query(insert_query, insert_values);
    // console.log('%c 🥜 update_result: ', 'font-size:20px;background-color: #F5CE50;color:#fff;', update_result);
    // console.log('%c 🍪 insert_result: ', 'font-size:20px;background-color: #E41A6A;color:#fff;', insert_result);

    // const get_by_filter = await client.query(`SELECT * FROM "roles" WHERE`)
    // console.log('%c 🍥 get_by_filter: ', 'font-size:20px;background-color: #4b4b4b;color:#fff;', get_by_filter);
    // const res = await client.query('SELECT $1::text as message', ['Hello world!'])

    // console.log(res.rows[0].message) // Hello world!

    const index_name = "gorentals_core_db_v4-vehicle";
    const insert_data = {
      id: "364424f7-50a3-4c46-a279-66fb5fb05245",
      type: "hehe_core_schema",
      tombstone: 0,
      status: "Active",
      config: {
        entity: 151,
        type: "yohoo",
        created_date: 1651728063044,
        random: NaN,
      },
      created_date: 1651728063045,
      updated_date: 1651728063049,
      version: 1,
      schema_version: 4,
    };
    const vehicle_data = {
      id: "8e2f3023-649c-4c27-b855-5f98d0894657",
      status: "Active",
      vehicle_category_id: "a63c44cf-586c-4970-8727-283c3c70be52",
      vid: "",
      vin: "WP1AA2AY8KDA15633",
      vehicle_class_id: "5b693a35-d38b-4eaf-ba1a-1aa7993ad344",
      vehicle_rate_id: "a63c44cf-586c-4970-8727-283c3c70be52",
      year: 2019,
      make_id: "a63c44cf-586c-4970-8727-283c3c70be52",
      model_id: "a63c44cf-586c-4970-8727-283c3c70be52",
      trim_id: "a63c44cf-586c-4970-8727-283c3c70be52",
      lot_id: "a63c44cf-586c-4970-8727-283c3c70be52",
      counter_id: "e3107bbf-d20e-4c07-a586-6ceab0839e35",
      vehicle_status: "Available",
      image_url: "",
      odometer: 0,
      exterior_color: "Black",
      interior_color: "Brown",
      make_name: "",
      model_name: "",
      trim_name: "",
      attribute: {
        specs: {
          Options: {
            "Transmission Style": {
              value_type: "text",
              value_type_label: "Text",
              value: "Automatic",
            },
            "Number of Seats": {
              value_type: "text",
              value_type_label: "Text",
              value: "7",
            },
            "Number of Luggage": {
              value_type: "text",
              value_type_label: "Text",
              value: "6",
            },
            "Number of Doors": {
              value_type: "text",
              value_type_label: "Text",
              value: "4",
            },
          },
          "Additional Features": {
            "Motorcycle Chassis Type": {
              value_type: "text",
              value_type_label: "Text",
              value: "Not Applicable",
            },
            "Fuel Type - Primary": {
              value_type: "text",
              value_type_label: "Text",
              value: "Gasoline",
            },
            "Seat Belt Type": {
              value_type: "text",
              value_type_label: "Text",
              value: "Manual",
            },
            "Engine Number of Cylinders": {
              value_type: "text",
              value_type_label: "Text",
              value: "6",
            },
            "Model Year": {
              value_type: "text",
              value_type_label: "Text",
              value: "2019",
            },
            "Displacement (L)": {
              value_type: "text",
              value_type_label: "Text",
              value: "3",
            },
            "Front Air Bag Locations": {
              value_type: "text",
              value_type_label: "Text",
              value: "1st Row (Driver and Passenger)",
            },
            "Trailer Type Connection": {
              value_type: "text",
              value_type_label: "Text",
              value: "Not Applicable",
            },
            "Displacement (CC)": {
              value_type: "text",
              value_type_label: "Text",
              value: "3000",
            },
            Doors: {
              value_type: "text",
              value_type_label: "Text",
              value: "4",
            },
            "Displacement (CI)": {
              value_type: "text",
              value_type_label: "Text",
              value: "183.0712322841",
            },
            "Tire Pressure Monitoring System (TPMS) Type": {
              value_type: "text",
              value_type_label: "Text",
              value: "Direct",
            },
            "Body Class": {
              value_type: "text",
              value_type_label: "Text",
              value: "Sport Utility Vehicle (SUV)/Multi-Purpose Vehicle (MPV)",
            },
            "Engine Power (kW)": {
              value_type: "text",
              value_type_label: "Text",
              value: "249.8095",
            },
            "Trailer Body Type": {
              value_type: "text",
              value_type_label: "Text",
              value: "Not Applicable",
            },
            "Custom Motorcycle Type": {
              value_type: "text",
              value_type_label: "Text",
              value: "Not Applicable",
            },
            "Plant City": {
              value_type: "text",
              value_type_label: "Text",
              value: "BRATISLAVA",
            },
            "Vehicle Type": {
              value_type: "text",
              value_type_label: "Text",
              value: "MULTIPURPOSE PASSENGER VEHICLE (MPV)",
            },
            "Engine Brake (hp) From": {
              value_type: "text",
              value_type_label: "Text",
              value: "335",
            },
            Make: {
              value_type: "text",
              value_type_label: "Text",
              value: "PORSCHE",
            },
            "Manufacturer Name": {
              value_type: "text",
              value_type_label: "Text",
              value: "DR. ING. H.C.F. PORSCHE AG",
            },
            "Side Air Bag Locations": {
              value_type: "text",
              value_type_label: "Text",
              value: "1st Row (Driver and Passenger)",
            },
            "Plant Country": {
              value_type: "text",
              value_type_label: "Text",
              value: "SLOVAKIA",
            },
            Series: {
              value_type: "text",
              value_type_label: "Text",
              value: "Type 9YA",
            },
            "Bus Floor Configuration Type": {
              value_type: "text",
              value_type_label: "Text",
              value: "Not Applicable",
            },
            "Bus Type": {
              value_type: "text",
              value_type_label: "Text",
              value: "Not Applicable",
            },
            "Engine Manufacturer": {
              value_type: "text",
              value_type_label: "Text",
              value: "Dr. Ing. h.c.F. Porsche AG",
            },
            "Curtain Air Bag Locations": {
              value_type: "text",
              value_type_label: "Text",
              value: "1st and 2nd Rows",
            },
            Model: {
              value_type: "text",
              value_type_label: "Text",
              value: "Cayenne",
            },
            "Gross Vehicle Weight Rating From": {
              value_type: "text",
              value_type_label: "Text",
              value: "Class 2E: 6,001 - 7,000 lb (2,722 - 3,175 kg)",
            },
            "Motorcycle Suspension Type": {
              value_type: "text",
              value_type_label: "Text",
              value: "Not Applicable",
            },
          },
        },
        vehicle_class_id: "5b693a35-d38b-4eaf-ba1a-1aa7993ad344",
        model_name: "",
        vehicle_class_name: "",
        vehicle_category_id: "a63c44cf-586c-4970-8727-283c3c70be52",
        vehicle_status: "Available",
        active_scene: 1649147181514,
        model_id: "",
        make_name: "",
        make_id: "a63c44cf-586c-4970-8727-283c3c70be52",
        fuel_type: "Gasoline",
        status: "Active",
      },
      code: "VE100005",
      entity_code: "",
      files: [
        {
          value_type: "text",
          value_type_label: "Text",
          value: "Not Applicable",
        },
        {
          valuex_type: "ht",
          value_type_label: "Text",
          value: "Not Applicable",
        },
      ],
    };
    const {
      fields,
      set_values,
      values: insert_values,
    } = getPostgresInsertQuery(vehicle_data);
    console.log(
      "%c 🌰 getPostgresInsertQuery(insert_data);: ",
      "font-size:20px;background-color: #4b4b4b;color:#fff;",
      getPostgresInsertQuery(insert_data)
    );

    console.log(
      "%c 🍐 insert_values: ",
      "font-size:20px;background-color: #F5CE50;color:#fff;",
      insert_values
    );
    const insert_query_data = `
    INSERT INTO "${index_name}"
    (${fields.toString()})
    VALUES (${set_values});
    `;

    await client.query(insert_query_data, insert_values);

    await client.end();
  } catch (error: any) {
    throw new Error(`MAIN ERROR : ${error}`);
  }
};

main();
