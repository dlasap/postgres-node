
export const tableSchemaBuilder = async (entity: any) => {
    let query = "";
    entity.fields?.map((field: any) => {
      query =
        query +
        `${field.field_name} ${field.data_type}${
          field.is_primary_key ? " PRIMARY KEY" : ""
        },`;
    });
    // Check if primary_keys props is available
    if (entity.primary_keys?.length) {
      return query + `PRIMARY KEY (${entity.primary_keys.join(",")})`;
    }
  
    return query.slice(0, -1);
  };
  