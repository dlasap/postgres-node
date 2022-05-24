import Sync from "../Core/Sync";
import { Timestamp } from "../Core/Timestamp";
import uuidv4 from "../../utils/uuidv4";
import {
  Message,
  StoreConfig,
  OperationTable,
  StoreItem,
  StoreAccessCollectionParameters,
  StoreAccessItemParameters,
  StoreAccessByFilterParameters,
  StoreAccessByIndexParameters,
  SystemQueryOperations,
  SystemUpdateOperations,
  StoreAccessOperations,
  StoreTransactionOperations,
  SystemTransactionsOperations,
  ElasticSeachOperations,
  ISingleFilterCriteriaRequest,
  ISearchParams,
  IMigrationOperations,
  StoreAccessBaseParameters,
} from "../types";
import { getSystemId } from "./utils/file";
const {
  SERVICE_ID = "1",
  SAVE_SYSTEM_ID_TO = "file", // file, redis_db
  COMPANY_NAME = "core",
} = process.env;
/**
 * * Simple Storage Class that implements a Grow-Only Set(G-Set) of Last-Write-Wins(LWW) mutations. 
 * * LWW is evaluated using a merkle tree by parent class. (See class: Sync)
 * * Usage: Use the class as is or extend implement interfaces to other backing storage.
 * * IMPORTANT - This is a partial class. Extend, Override and implement missing functions.
 @class PartialStore
 @extends Sync
 @param {object} config typeof StoreConfig
*/
class PartialStore
  extends Sync
  implements
    SystemQueryOperations,
    SystemUpdateOperations,
    StoreAccessOperations,
    SystemTransactionsOperations,
    StoreTransactionOperations,
    ElasticSeachOperations,
    IMigrationOperations
{
  state?: any;
  current_entity_system_id: Record<string, any>;
  current_record_id: string;
  constructor({ initial_state = {}, transport, message_store }: StoreConfig) {
    super(transport, message_store);
    this.current_entity_system_id = {};
    this.current_record_id = "";
    if (initial_state) {
      this.state = initial_state;
    } else {
      this.state = {
        user: [],
        company: [],
      };
    }
  }
  /*
    Pass all mutations to the Sync Parent using sendMessage.
    The Sync Layer decides whether to apply the operation or not
    based on the result of the position of the timestamp in the merkle tree.
    When Sync decides to do the operation, it will call the 'apply' method. ( see apply below. )
  */

  async insert<TRowSchema = any>(
    database: string,
    dataset: string,
    row: TRowSchema
  ) {
    let id = uuidv4();
    // @ts-ignore
    const row_id = row?.id || id;

    // from redis disk
    const data = await getSystemId({
      get_to: SAVE_SYSTEM_ID_TO,
      company_name: COMPANY_NAME,
      dataset,
      current_record_id: this.current_record_id,
      row_id,
    });

    let fields: any = Object.keys({ ...row, tombstone: 0 });
    const transaction_id = uuidv4();

    // Break down row operations into atomic row-column operations and apply them separately.
    // LWW wins on a per column basis.
    // Updates against keys within the same row must be mutually exclusive.

    if (this.current_record_id !== row_id) {
      this.current_record_id = id;
    }

    const mapInsertMessages = (k: string): Message => {
      const message: Message = {
        id: uuidv4(),
        transaction_id,
        system_id: data,
        service_id: SERVICE_ID,
        operation: "update",
        database,
        dataset,
        row: (row as any).id || id,
        column: k,
        value: (row as any)[k],
        timestamp: Timestamp.send(this.clock.getClock()).toString(),
        entity_fields: row,
      };

      return message;
    };

    const mapped_messages = fields.map(mapInsertMessages);
    await this.sendMessages(mapped_messages);
    await this.sendIndex({ ...mapped_messages[0] });

    return id;
  }

  async update(database: string, dataset: string, params: any) {
    // Same as insert but remove id from key to be updated.
    const fields = Object.keys(params).filter((k) => k !== "id");
    const transaction_id = uuidv4();
    const mapUpdateMessages = (k: string): Message => {
      return {
        id: uuidv4(),
        service_id: SERVICE_ID,
        transaction_id,
        system_id: params.system_id,
        operation: "update",
        dataset,
        database,
        row: params.id,
        column: k,
        value: params[k],
        timestamp: Timestamp.send(this.clock.getClock()).toString(),
        entity_fields: params,
      };
    };

    const mapped_messages = fields.map(mapUpdateMessages);
    await this.sendMessages(mapped_messages);
    await this.sendIndex({
      ...mapped_messages[0],
    });

    return params.id;
  }

  async delete(database: string, dataset: string, id: string) {
    const transaction_id = uuidv4();

    // CRDT implements soft delete by default.
    // In an eventually consistent system(even the strong ones), you can never safely remove items.
    // Soft delete ensures that updates to the record are still applied when updates come in out of order after the delete operation has been applied.
    // Tombstones are placed as markers for rows that have been softly deleted.

    const mapped_messages: Array<Message> = [
      {
        id: uuidv4(),
        transaction_id,
        service_id: SERVICE_ID,
        operation: "update",
        database,
        dataset,
        row: id,
        column: "tombstone",
        value: 1,
        timestamp: Timestamp.send(this.clock.getClock()).toString(),
        entity_fields: { id },
      },
    ];

    await this.sendMessages(mapped_messages);
    await this.sendIndex({
      ...mapped_messages[0],
    });

    return id;
  }

  async apply(msg: Message): Promise<void> {
    const operations: OperationTable = {
      database_create: async () => this.applyCreateDatabase(msg.database),
      table_create: async () =>
        this.applyCreateTable(msg.database, msg.dataset, msg.options),
      index_create: async () =>
        this.applyCreateIndex(msg.database, msg.dataset, msg.column),
      update: async () => this.applyUpdate(msg),
    };
    await operations[msg.operation]();
  }

  async createDatabase(database: string) {
    let id = uuidv4();
    const transaction_id = uuidv4();

    // Break down row operations into atomic row-column operations and apply them separately.
    // LWW wins on a per column basis.
    // Updates against keys within the same row must be mutually exclusive.
    const message: Message = {
      id: uuidv4(),
      transaction_id,
      service_id: SERVICE_ID,
      operation: "database_create",
      database,
      dataset: "",
      row: "",
      column: "",
      value: "",
      timestamp: Timestamp.send(this.clock.getClock()).toString(),
      entity_fields: {},
    };
    await this.sendMessages([message]);
    await this.sendIndex(message);
    return id;
  }

  async createTable(database: string, dataset: string, options?: any) {
    let id = uuidv4();
    const transaction_id = uuidv4();

    // Break down row operations into atomic row-column operations and apply them separately.
    // LWW wins on a per column basis.
    // Updates against keys within the same row must be mutually exclusive.
    const message: Message = {
      id: uuidv4(),
      transaction_id,
      service_id: SERVICE_ID,
      operation: "table_create",
      database,
      dataset,
      row: "",
      column: "",
      value: "",
      timestamp: Timestamp.send(this.clock.getClock()).toString(),
      entity_fields: {},
      options: options || {},
    };
    await this.sendMessages([message]);
    await this.sendIndex(message);
    return id;
  }

  async createIndex(database: string, dataset: string, index: string) {
    let id = uuidv4();
    const transaction_id = uuidv4();

    // Break down row operations into atomic row-column operations and apply them separately.
    // LWW wins on a per column basis.
    // Updates against keys within the same row must be mutually exclusive.
    const message: Message = {
      id: uuidv4(),
      transaction_id,
      service_id: SERVICE_ID,
      operation: "index_create",
      database,
      dataset,
      row: "",
      column: index,
      value: "",
      timestamp: Timestamp.send(this.clock.getClock()).toString(),
      entity_fields: {},
    };
    await this.sendMessages([message]);
    await this.sendIndex(message);
    return id;
  }

  /*
    After evaluating the position of the message's timestamp within the merkle tree,
    If the the operation is deemed to be the last update - This apply function  is called.
    Override and implement this function to match the operations of your backing store.
  */
  async applyCreateDatabase(database: string, options?: any): Promise<void> {
    throw new Error(`Create Database Not Implemented`);
  }

  async getMapping(index: string): Promise<Record<string, any>> {
    throw new Error(`Get Mapping Not Implemented`);
  }

  async count(_: StoreAccessBaseParameters): Promise<number> {
    throw new Error(`Get count Not Implemented`);
  }

  async paginatedResults(_: StoreAccessByFilterParameters<any>): Promise<any> {
    throw new Error(`Get count Not Implemented`);
  }

  async applyCreateTable(
    database: string,
    table: string,
    options?: any
  ): Promise<void> {
    throw new Error(`Create Table Not Implemented`);
  }
  async applyCreateIndex(
    database: string,
    dataset: string,
    column: string
  ): Promise<void> {
    throw new Error(`Create Index Not Implemented`);
  }
  async applyUpdate(msg: Message) {
    throw new Error(`applyUpdate Not Implemented`);
  }
  async listDatabases(): Promise<string[]> {
    throw new Error(`listDatabases Not Implemented`);
  }
  async listTables(database: string): Promise<string[]> {
    throw new Error(`listTables Not Implemented`);
  }
  async listIndex(database: string, dataset: string): Promise<string[]> {
    throw new Error(`listTables Not Implemented`);
  }
  /*
    When accessing data, you can still query them as is with the exception of filtering out tombstones.
  */
  async getById(params: StoreAccessItemParameters): Promise<StoreItem | null> {
    throw new Error(`getById Not Implemented`);
  }
  async list(params: StoreAccessCollectionParameters): Promise<StoreItem[]> {
    throw new Error(`list Not Implemented`);
    //@ts-ignore
    return null;
  }
  async getByIndex(
    params: StoreAccessByIndexParameters<any>
  ): Promise<StoreItem[]> {
    throw new Error(`getByIndex Not Implemented`);
    //@ts-ignore
    return null;
  }
  async getByFilter(
    params: StoreAccessByFilterParameters<any>
  ): Promise<StoreItem[]> {
    throw new Error(`getByFilter Not Implemented`);
    //@ts-ignore
    return null;
  }
  async globalSearch(params: any): Promise<any> {
    throw new Error(`globalSearch Not Implemented`);
    //@ts-ignore
    return null;
  }
  async entitySearch(params: any): Promise<any> {
    throw new Error(`elasticSearch Not Implemented`);
    //@ts-ignore
    return null;
  }
  async getVehicleRateByClass(params: any): Promise<any> {
    throw new Error(`elasticSearch Not Implemented`);
    //@ts-ignore
    return null;
  }
  async getByMultipleFilter(params: any): Promise<any> {
    throw new Error(`elasticSearch Not Implemented`);
    //@ts-ignore
    return null;
  }
  async getNearbyLocation(params: any): Promise<any> {
    throw new Error(`elasticSearch Not Implemented`);
    //@ts-ignore
    return null;
  }
  async getBySingleFilter(params: ISingleFilterCriteriaRequest): Promise<any> {
    throw new Error(`elasticSearch Not Implemented`);
    //@ts-ignore
    return null;
  }
  async search(params: ISearchParams): Promise<StoreItem[]> {
    throw new Error("Search Not Implemented");
    return [];
  }
}

export default PartialStore;
