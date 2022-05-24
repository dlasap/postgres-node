import { IStatuses } from "../models/types";
import { isArray, isObject } from "lodash";

const getCapitalFirstLetter = (str: string) => {
  return str.charAt(0).toUpperCase();
};

const getFormattedStr = (
  str: string,
  { target = "", replaceTo = "", isPascalCase = false }
) => {
  const str_replaced = str.replace(target, replaceTo).replace(/[0-9]/g, "");

  if (isPascalCase) {
    const array_str = str_replaced.split("_");
    return array_str
      .map(
        (str: string) => getCapitalFirstLetter(str) + str.substr(1, str.length)
      )
      .join("");
  }

  return (
    getCapitalFirstLetter(str_replaced) +
    str_replaced.substr(1, str_replaced.length)
  );
};

const getDepth = (value: Record<string, any | any[]>): number => {
  return isArray(value) || isObject(value)
    ? 1 +
        Math.max(
          0,
          ...Object.keys(value).map((e) =>
            getDepth(value[e as keyof typeof value])
          )
        )
    : 0;
};

const isUUID = (input: string, version?: number) => {
  //REGEX patterns per version
  version = version || 4;
  const uuid_patterns: Record<number, any> = {
    1: /^[0-9A-F]{8}-[0-9A-F]{4}-1[0-9A-F]{3}-[0-9A-F]{4}-[0-9A-F]{12}$/i,
    2: /^[0-9A-F]{8}-[0-9A-F]{4}-2[0-9A-F]{3}-[0-9A-F]{4}-[0-9A-F]{12}$/i,
    3: /^[0-9A-F]{8}-[0-9A-F]{4}-3[0-9A-F]{3}-[0-9A-F]{4}-[0-9A-F]{12}$/i,
    4: /^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i,
    5: /^[0-9A-F]{8}-[0-9A-F]{4}-5[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i,
  };

  if (typeof input === "string") {
    if (
      Object.keys(uuid_patterns).includes(
        typeof version === "string" ? version : String(version)
      )
    ) {
      return uuid_patterns[version].test(input);
    } else {
      return Object.values(uuid_patterns).some((pattern) =>
        pattern.test(input)
      );
    }
  }
  return false;
};

const restructureNestedObject = async (
  object: Record<string, any>
): Promise<Record<string, any>> => {
  let restructured_obj: Record<string, any> = {};
  const recurse = (
    field_name: string,
    object: Record<string, any>,
    is_obj_inside_array: boolean
  ) => {
    if (Object.keys(object).length) {
      Object.entries(object).forEach(([key, value]) => {
        if (isObject(value) && !isArray(value)) {
          return recurse(field_name + "." + key, value, is_obj_inside_array);
        } else if (isArray(value) && isObject(value[0])) {
          // if array of object, array_obj = [{},{}]

          recurseArray(value, field_name + "." + key);
        } else if (is_obj_inside_array) {
          const curr_key = field_name + "." + key;
          let fitlered_value = isArray(value)
            ? value.filter(
                (item: any) => !restructured_obj[curr_key]?.includes(item)
              )
            : restructured_obj[curr_key]?.includes(value)
            ? []
            : [value];
          restructured_obj = {
            ...restructured_obj,
            [curr_key]: restructured_obj[curr_key]
              ? [...restructured_obj[curr_key], ...fitlered_value]
              : isArray(value)
              ? value
              : [value],
          };
        } else {
          restructured_obj = {
            ...restructured_obj,
            [field_name + "." + key]: value,
          };
          return;
        }
      });
    } else {
      restructured_obj = { ...restructured_obj, [field_name]: {} };
    }
  };

  // to recurse if array ang certain field
  const recurseArray = (
    objects: Record<string, any>[],
    curr_field_name: string
  ) => {
    let initial_data_keys = Object.keys(objects[0]);

    objects.forEach((item) => {
      const data_keys = Object.keys(item);
      data_keys.forEach((arr_obj_key) => {
        const new_field_name = `${curr_field_name}.${arr_obj_key}`;

        if (!initial_data_keys.includes(arr_obj_key)) {
          initial_data_keys = [...initial_data_keys, arr_obj_key];
        }
        let data = item[arr_obj_key];

        if (isArray(item[arr_obj_key]) && !isObject(item[arr_obj_key][0])) {
          // data = ['',''] | [0,1]
          data = data.flat();
        }
        if (isObject(item[arr_obj_key]) && !isArray(item[arr_obj_key])) {
          // if { data: {}}
          recurse(new_field_name, item[arr_obj_key], true);
        } else if (
          isArray(item[arr_obj_key]) &&
          isObject(item[arr_obj_key][0])
        ) {
          // if { data: [{},{}]}
          recurseArray(item[arr_obj_key], new_field_name);
        } else if (restructured_obj[new_field_name]) {
          data = isArray(data)
            ? data.filter(
                (item: any) => !restructured_obj[new_field_name].includes(item)
              )
            : restructured_obj[new_field_name].includes(data)
            ? []
            : [data];
          restructured_obj[new_field_name] = [
            ...restructured_obj[new_field_name],
            ...data,
          ];
        } else {
          // if wala pa mo exist
          restructured_obj = {
            ...restructured_obj,
            [new_field_name]: isArray(data) ? data : [data],
          };
        }
      });
    });
  };
  Object.entries(object).forEach(([key, value]) => {
    if (isObject(value) && !isArray(value)) {
      recurse(key, value, false);
    }
    //tombstone needed for db
    // else if (key === "tombstone") return;
    else if (isArray(value) && isObject(value[0])) {
      recurseArray(value, key);
    } else {
      restructured_obj = { ...restructured_obj, [key]: value };
    }
  });
  return {
    ...object,
    ...restructured_obj,
  };
};
const getPostgresUpdateQuery = async (payload: Record<string, any>) => {
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

const getPostgresInsertQuery = async (payload: any) => {
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

const getPostgresFilterQuery = async (filters: Record<string, any>) => {
  let filter_query = "";
  Object.entries(filters).forEach(([key, value]) => {
    filter_query
      ? (filter_query += ` AND ${key}='${value}'`)
      : (filter_query += `${key}='${value}'`);
  });
  return filter_query;
};

const createPostgresTableQuery = async (options: any) => {
  const { fields, primary_keys } = options;
  let formed_field_types: Array<string> = [];
  fields.forEach(({ field_name, data_type }: any) => {
    if (field_name === "user")
      formed_field_types.push(`"${field_name}" ${data_type}`);
    else {
      formed_field_types.push(
        `${field_name} ${data_type} ${
          primary_keys.includes(field_name) ? "PRIMARY KEY" : ""
        }`.trim()
      );
    }
  });
  return formed_field_types.toString();
};

export {
  getFormattedStr,
  createPostgresTableQuery,
  getPostgresInsertQuery,
  getPostgresUpdateQuery,
  getPostgresFilterQuery,
  getDepth,
  restructureNestedObject,
  isUUID,
};
