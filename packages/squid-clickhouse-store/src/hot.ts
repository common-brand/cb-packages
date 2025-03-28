import * as JSONbig from "@andreybest/json-bigint";
import { ClickhouseConnection } from "@cbts/clickhouse";
import {Debugger} from "debug";

import { ClickhouseTableConfig } from "./database";


export interface RowRef {
  table: string;
  id: string;
}

export interface InsertRecord extends RowRef {
  kind: "insert";
}

export interface DeleteRecord extends RowRef {
  kind: "delete";
  fields: Record<string, unknown>;
}

export interface UpdateRecord extends RowRef {
  kind: "update";
  fields: Record<string, unknown>;
}

export type ChangeRecord = InsertRecord | UpdateRecord | DeleteRecord;

export interface ChangeRow {
  height: number;
  log_index: number;
  changes: ChangeRecord;
}

export class ChangeTracker {
  private index = 0;

  constructor(
    private client: ClickhouseConnection,
    private statusSchema: string,
    private blockHeight: number,
    private debug: Debugger
  ) {
  }

  trackInsert(tableName: string, ids: string[]): Promise<void> {
    return this.writeChangeRows(
      ids.map((id) => {
        return {
          kind: "insert",
          table: tableName,
          id,
        };
      }),
    );
  }

  async trackDelete(tableName: string, ids: string[]): Promise<void> {
    return this.writeChangeRows(
      ids.map((id) => {
        // const { id, ...fields } = e;
        return {
          kind: "delete",
          table: tableName,
          id,
          fields: {},
        };
      }),
    );
  }

  private async writeChangeRows(changes: ChangeRecord[]): Promise<void> {
    await insertHotChangeLog(
      this.statusSchema,
      this.client,
      changes.map((i) => {
        return {
          changes: i,
          height: this.blockHeight,
          log_index: this.index++,
        };
      }),
        this.debug
    );
  }
}

export async function insertHotChangeLog(statusSchema: string, client: ClickhouseConnection, changes: ChangeRow[], debug: Debugger): Promise<void> {
  const values: { height: number; log_index: number; changes: string }[] = changes.map((i) => {
    return {
      height: i.height,
      log_index: i.log_index,
      changes: JSONbig.stringify(i.changes),
    };
  });
  await client.insert(`${statusSchema}.hot_change_log`, values, debug);
}

export async function rollbackBlock(
  statusSchema: string,
  client: ClickhouseConnection,
  blockHeight: number,
  chainId: number,
  tableConfig: ClickhouseTableConfig,
  debug: Debugger
): Promise<void> {
  const hotChangeLog = await client.query<{ height: number; log_index: number; changes: string }>(
    `SELECT height, log_index, changes
     FROM ${statusSchema}.hot_change_log FINAL
     WHERE height = ${blockHeight}
       AND chain_id = ${chainId}
     ORDER BY log_index DESC
    `,
    debug,
  );

  for (const rec of hotChangeLog) {
    const { table, id, kind } = JSONbig.parse(rec.changes);
    const trackBy = tableConfig[table]?.trackBy;

    switch (kind) {
      case "insert":
        await client.query(
          `DELETE
           FROM ${table}
           ON CLUSTER cluster1
           WHERE ${trackBy} = ${id}
             AND chain_id = ${chainId}`,
          debug,
        );
        break;
    }
  }

  await client.exec(
    `DELETE
     FROM ${statusSchema}.hot_block
     ON CLUSTER cluster1
     WHERE height = ${blockHeight}
       AND chain_id = ${chainId}`,
    debug,
  );
}
