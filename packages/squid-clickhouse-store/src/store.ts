import { ClickhouseConnection } from "@cbts/clickhouse";
import { ClickHouseSettings } from "@clickhouse/client";
import { Debugger } from "debug";

import { ClickhouseTableConfig } from "./database";
import { ChangeTracker } from "./hot";

export interface EntityClass<T> {
  new (): T;
}

export class Store {
  private records: Record<string, unknown[]>;

  constructor(
    public client: ClickhouseConnection,
    private changeTracker?: ChangeTracker,
    private tablesConfig?: ClickhouseTableConfig,
  ) {
    this.records = {};
  }

  async insert<T = unknown>(
    table: string,
    values: ReadonlyArray<T>,
    debug?: Debugger,
    log_comment?: string,
    clickhouse_settings?: ClickHouseSettings,
  ): Promise<number> {
    if (!values?.length) {
      return 0;
    }
    const res = await this.client.insert(table, values, debug, log_comment, clickhouse_settings);
    const trackBy = this.tablesConfig?.[table]?.trackBy;
    if (this.changeTracker && trackBy) {
      const ids = values.map((i) => (i as Record<string, string>)[trackBy]).filter(Boolean);
      debug?.("HOT INSERT: %o", {
        table,
        ids,
      });
      await this.changeTracker.trackInsert(table, ids);
    }
    return res;
  }

  registerRecord(table: string, value: unknown) {
    if (!this.records[table]) {
      this.records[table] = [];
    }
    this.records[table].push(value);
  }

  async writeRecords(debug: Debugger) {
    if (Object.keys(this.records).length) {
      debug("write records %o", this.records);
    }
    for (const table of Object.keys(this.records)) {
      await this.insert(table, this.records[table], debug, table);
      const relatedDicts = this.tablesConfig?.[table].relatedDicts;
      if (relatedDicts?.length) {
        for (const dict of relatedDicts) {
          await this.client.reloadDictionary(dict);
        }
      }
    }
    this.records = {};
  }
}
