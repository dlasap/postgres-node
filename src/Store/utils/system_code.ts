import createLogger from "../../../utils/logger";
import { IGetCode, ISystemCode } from "../../types";
const logger = createLogger("store:redis");
const { SYSTEM_COMPANY_KEY = "", SYSTEM_CODE_KEY = "" } = process.env;
export const getCode = async ({
  dataset = "",
  system_id = "",
  column = "",
  value = "",
  system_code,
  entity_fields,
}: IGetCode) => {
  let system_entity_code: any = {};
  if (column === "system_company_id" && SYSTEM_COMPANY_KEY !== value)
    logger.error(
      `system_company_id does not match with the company's SYSTEM_COMPANY_KEY.`
    );

  if (!SYSTEM_CODE_KEY) logger.error(`SYSTEM_CODE_KEY is empty.`);

  if (SYSTEM_CODE_KEY && system_code) {
    system_entity_code = system_code;
    let {
      name = "",
      prefix_code = "",
      number_scheme = "",
      suffix_code = [],
      default_code_number = 0,
    }: ISystemCode = system_entity_code;
    if (number_scheme === "incremental") {
      let parsed_system_id = Number(system_id);
      let incremental_complete_number = default_code_number + parsed_system_id;
      let code = entity_fields?.code;

      if (!code)
        code = `${prefix_code}${incremental_complete_number}${suffix_code.join(
          ""
        )}`;

      // !hot fix - booking has a custom system code based on category
      if (dataset === "booking") {
        const new_suffix_code =
          entity_fields?.attribute?.category.charAt(0).toUpperCase() ?? "Q";
        code = `${code.slice(0, code.length - 1)}${new_suffix_code}`;
      }

      logger.info(`[OVVERIDE]: ${name} - code into [${code}] `);
      return code;
    }
  }

  return null;
};
