import { ClickHouseClient, ClickHouseClientConfigOptions, ClickHouseSettings, createClient } from "@clickhouse/client";
import { CommandResult } from "@clickhouse/client-common";
import Debug, { Debugger } from "debug";

import { constants } from "./constants";

const debug = Debug("cbts:clickhouse.ts");

export class ClickhouseTimer {
  private clickhouse: ClickhouseConnection;
  private adj_interval: number;

  constructor(clickhouse: ClickhouseConnection) {
    this.clickhouse = clickhouse;
    this.adj_interval = 0;
  }

  next(): Promise<number> {
    let timestamp;
    if (this.adj_interval < constants.ONE_DAY_IN_SECONDS) {
      timestamp = this.clickhouse.toStartOf("30 MINUTE", `${this.adj_interval} SECOND`);
      this.adj_interval += 30 * constants.ONE_MINUTE_IN_SECONDS;
    } else if (this.adj_interval < constants.ONE_WEEK_IN_SECONDS) {
      timestamp = this.clickhouse.toStartOf("2 HOUR", `${this.adj_interval} SECOND`);
      this.adj_interval += 2 * constants.ONE_HOUR_IN_SECONDS;
    } else if (this.adj_interval < 31 * constants.ONE_DAY_IN_SECONDS) {
      timestamp = this.clickhouse.toStartOf("8 HOUR", `${this.adj_interval} SECOND`);
      this.adj_interval += 8 * constants.ONE_HOUR_IN_SECONDS;
    } else {
      timestamp = this.clickhouse.toStartOf("1 DAY", `${this.adj_interval} SECOND`);
      this.adj_interval += constants.ONE_DAY_IN_SECONDS;
    }
    return timestamp;
  }
}

export class ClickhouseConnection {
  static MAX_INSERT_ROWS = 100_000;
  public writer: ClickHouseClient;
  public reader: ClickHouseClient;

  constructor(settings: ClickHouseSettings = {}) {
    const commons: ClickHouseSettings = {
      async_insert: 1,
      lightweight_deletes_sync: '2',
      mutations_sync: '2',
      wait_for_async_insert: 1,
      apply_mutations_on_fly: 1,
    };
    const envReader: ClickHouseClientConfigOptions = {
      ...ClickhouseConnection.getClickHouseConfig(),
      clickhouse_settings: {
        readonly: "1",
        ...commons,
        ...settings
      },
      request_timeout: 60_000,
    };
    const envWriter: ClickHouseClientConfigOptions = {
      ...ClickhouseConnection.getClickHouseConfig(),
      clickhouse_settings: {
        ...commons,
        ...settings
      },
      request_timeout: 60_000,
    };
    debug("writer: %j reader: %j", settings, envReader);
    this.writer = createClient(envWriter);
    this.reader = createClient(envReader);
  }

  static getClickHouseConfig(): ClickHouseClientConfigOptions {
    return {
      url: process.env["CLICKHOUSE_URL"],
      username: process.env["CLICKHOUSE_USER"] || "default",
      password: process.env["CLICKHOUSE_PASSWORD"] || "",
    };
  }

  async insertBatchValues<T = unknown>(
    table: string,
    values: ReadonlyArray<T>,
    debug?: Debugger,
    log_comment?: string,
    clickhouse_settings?: ClickHouseSettings,
  ): Promise<void> {
    const batchSize = ClickhouseConnection.MAX_INSERT_ROWS - 1;
    for (let i = 0; i < values.length; i += batchSize) {
      const batch = values.slice(i, i + batchSize);
      const { query_id } = await this.writer.insert<T>({
        table,
        values: batch,
        format: "JSONEachRow",
        clickhouse_settings,
      });
      this.processDebug(`inserted into ${table} rows: ${batch.length}`, query_id, debug, log_comment);
    }
  }

  async insert<T = unknown>(
    table: string,
    values: ReadonlyArray<T>,
    debug?: Debugger,
    log_comment?: string,
    clickhouse_settings?: ClickHouseSettings,
  ): Promise<number> {
    const total = values.length;
    if (values.length > 0) {
      await this.insertBatchValues(table, values, debug, log_comment, clickhouse_settings);
    }
    return total;
  }

  /**
   * Should be used with query like INSERT INTO SELECT ...
   * @return number of inserted rows
   */
  async insertSelected(
    query: string,
    debug?: Debugger,
    log_comment?: string,
    query_params?: Record<string, unknown>,
    clickhouse_settings?: ClickHouseSettings,
  ): Promise<number> {
    const res = await this.writer.exec({
      query,
      query_params,
      clickhouse_settings,
    });
    const data = res.summary;
    const total = Number.parseInt(data?.result_rows || "0");
    this.processDebug(` query: [${query}] rows: ${total}`, res.query_id, debug, log_comment);
    return total;
  }

  async reloadDictionary(table: string, debug?: Debugger, log_comment?: string): Promise<CommandResult> {
    const query = `SYSTEM RELOAD DICTIONARY ${table} ON CLUSTER cluster1`;
    const res = await this.writer.exec({
      query,
      clickhouse_settings: { log_comment },
    });
    this.processDebug(`query: [${query}]`, res.query_id, debug, log_comment);
    return res;
  }

  async exec(
    query: string,
    debug?: Debugger,
    log_comment?: string,
    query_params?: Record<string, unknown>,
    clickhouse_settings?: ClickHouseSettings,
  ): Promise<CommandResult> {
    const res = await this.writer.exec({
      query,
      query_params,
      clickhouse_settings,
    });
    this.processDebug(`query: [${query}]`, res.query_id, debug, log_comment);
    return res;
  }

  private processDebug(message: string, query_id?: string, debug?: Debugger, log_comment?: string): void {
    if (debug) {
      const queryId = query_id ? `{${query_id}} ` : "";
      const q = message.replace(/\s+/g, " ");
      if (log_comment) {
        debug("%s%s(): %s", queryId, log_comment, q);
      } else {
        debug("%s%s", queryId, q);
      }
    }
  }

  async query<T = unknown>(
    query: string,
    debug?: Debugger,
    log_comment?: string,
    clickhouse_settings?: ClickHouseSettings,
    query_params?: Record<string, unknown>,
  ): Promise<T[]> {
    const response = await this.reader.query({
      query,
      format: "JSONEachRow",
      query_params,
      clickhouse_settings: { log_comment, ...clickhouse_settings },
    });
    const result = await response.json<T>();
    this.processDebug(`query=[${query}] rows=${result.length}`, undefined, debug, log_comment);
    return result;
  }

  async queryRow<T = unknown>(
    query: string,
    debug?: Debugger,
    log_comment?: string,
    clickhouse_settings?: ClickHouseSettings,
    query_params?: Record<string, unknown>,
  ): Promise<T> {
    const rows = await this.query<T>(query, debug, log_comment, clickhouse_settings, query_params);
    return rows[0];
  }

  // async queryWithTotals<T = unknown>(
  //   query: string,
  //   debug?: Debugger,
  //   log_comment?: string,
  //   clickhouse_settings?: ClickHouseSettings,
  //   query_params?: Record<string, unknown>,
  // ): Promise<{ data: T[]; totals: T; total: number }> {
  //   const response = await this.reader.query({
  //     query,
  //     query_params,
  //     format: "JSON",
  //     clickhouse_settings: { log_comment, ...clickhouse_settings },
  //   });
  //   const { data, totals, rows_before_limit_at_least } = await response.json<{
  //     data: T[];
  //     totals: T;
  //     rows_before_limit_at_least: number;
  //   }>();
  //   this.processDebug(
  //     `query=[${query}] rows=${data.length} rows_before_limit_at_least=${rows_before_limit_at_least}`,
  //     undefined,
  //     debug,
  //     log_comment,
  //   );
  //   return {
  //     data,
  //     totals,
  //     total: rows_before_limit_at_least || data?.length,
  //   };
  // }

  async toStartOfInterval(adj_seconds: number, minutes: number): Promise<number> {
    const query = `select toUnixTimestamp(toStartOfInterval(now() - INTERVAL ${adj_seconds} second, INTERVAL ${minutes} minute)) date_adj`;
    const record = await this.queryRow<{ date_adj: number }>(query, debug, "toStartOfInterval");
    return record.date_adj;
  }

  /**
   * Get clickhouse timestamp adjusted to specific period
   */
  async toStartOf(interval: string, adj_interval = "0 second"): Promise<number> {
    const query = `select toUnixTimestamp(toStartOfInterval(now() - INTERVAL ${adj_interval}, INTERVAL ${interval})) date_adj`;
    const record = await this.queryRow<{ date_adj: number }>(query);
    return record.date_adj;
  }

  /**
   * timer user to generate expected timestamps to insert into day, week, month, year tables.
   */
  timer(): ClickhouseTimer {
    return new ClickhouseTimer(this);
  }
}

const clickhouse = new ClickhouseConnection();
export { clickhouse };
