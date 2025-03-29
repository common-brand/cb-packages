import {ClickhouseConnection} from "@cbts/clickhouse";
import {last, maybeLast} from "@subsquid/util-internal";
import {FinalTxInfo, HashAndHeight, HotDatabase, HotTxInfo} from "@subsquid/util-internal-processor-tools";
import assert from "assert";
import Debug, {Debugger} from "debug";

import {ChangeTracker, rollbackBlock} from "./hot";
import {DatabaseState} from "./interfaces";
import {Store} from "./store";

export type ClickhouseTableConfig = Record<string, { trackBy: string; relatedDicts?: string[] }>;

export interface ClickhouseDatabaseOptions {
  chainId: number;
  debug: Debugger;
  supportHotBlocks?: boolean;
  client: ClickhouseConnection;
  stateSchema: string;
  projectDir?: string;
  tablesConfig: ClickhouseTableConfig;
}

export class ClickhouseDatabase implements HotDatabase<Store> {
  private chainId = 0;
  private stateSchema: string;
  private client: ClickhouseConnection;
  private tablesConfig!: ClickhouseTableConfig;
  private debug;
  public supportsHotBlocks = true as const;

  constructor(options?: ClickhouseDatabaseOptions) {
    this.chainId = options?.chainId || 0;
    this.tablesConfig = options?.tablesConfig || ({} as ClickhouseTableConfig);
    this.client = options?.client || new ClickhouseConnection();
    this.stateSchema = options?.stateSchema || "";
    this.debug = options?.debug || Debug(`cbts:clickhouse-store-database`);
  }

  async connect(): Promise<DatabaseState> {
    const status = await this.getLastStatus();
    return {
      ...status,
      top: [],
    };
  }

  async transact(info: FinalTxInfo, cb: (_: Store) => Promise<void>): Promise<void> {
    this.debug("transact %o", info);
    const state = await this.getState();
    const { prevHead: prev, nextHead: next } = info;
    // state and info.prevHead must be the same
    assert(state.hash === prev.hash && state.height === prev.height, RACE_MSG);
    // check next block greater then prev
    assert(prev.height < next.height && prev.hash != next.hash);
    // rollback all top blocks
    for (let i = state.top.length - 1; i >= 0; i--) {
      const block = state.top[i];
      await rollbackBlock(this.stateSchema, this.client, block.height, this.chainId, this.tablesConfig, this.debug);
    }
    await this.performUpdates(cb);
    await this.updateStatus(next);
  }

  transactHot(info: HotTxInfo, cb: (store: Store, block: HashAndHeight) => Promise<void>): Promise<void> {
    return this.transactHot2(info, async (store, sliceBeg, sliceEnd) => {
      for (let i = sliceBeg; i < sliceEnd; i++) {
        await cb(store, info.newBlocks[i]);
      }
    });
  }

  async transactHot2(
    info: HotTxInfo,
    cb: (store: Store, sliceBeg: number, sliceEnd: number) => Promise<void>,
  ): Promise<void> {
    const state = await this.getState();
    let chain = [state, ...state.top];
    this.debug("transactHot2 %o", {
      info,
      stateHead: chain[0].height,
      rollbackPos: info.baseHead.height + 1 - chain[0].height,
      finalizeEnd: info.finalizedHead.height - info.newBlocks[0].height + 1,
    });

    assertChainContinuity(info.baseHead, info.newBlocks);
    assert(info.finalizedHead.height <= (maybeLast(info.newBlocks) ?? info.baseHead).height);
    // assert(
    //   chain.find((b) => b.hash === info.baseHead.hash),
    //   RACE_MSG,
    // );
    if (info.newBlocks.length == 0) {
      assert(last(chain).hash === info.baseHead.hash, RACE_MSG);
    }
    assert(chain[0].height <= info.finalizedHead.height, RACE_MSG);

    const rollbackPos = info.baseHead.height + 1 - chain[0].height;
    for (let i = chain.length - 1; i >= rollbackPos; i--) {
      await rollbackBlock(this.stateSchema, this.client, chain[i].height, this.chainId, this.tablesConfig, this.debug);
    }

    if (info.newBlocks.length) {
      let finalizedEnd = info.finalizedHead.height - info.newBlocks[0].height + 1;
      if (finalizedEnd > 0) {
        await this.performUpdates((store) => cb(store, 0, finalizedEnd));
      } else {
        finalizedEnd = 0;
      }

      for (let i = finalizedEnd; i < info.newBlocks.length; i++) {
        const b = info.newBlocks[i];
        await this.insertHotBlock(b);
        await this.performUpdates(
          (store) => cb(store, i, i + 1),
          new ChangeTracker(this.client, this.stateSchema, b.height, this.debug),
        );
      }
    }
    chain = chain.slice(0, rollbackPos).concat(info.newBlocks);
    const finalizedHeadPos = info.finalizedHead.height - chain[0].height;

    if (chain[finalizedHeadPos].hash != info.finalizedHead.hash) {
      this.debug("!!! chain[finalizedHeadPos].hash != info.finalizedHead.hash; %o", {
        finalizedHeadPos,
        "chain[finalizedHeadPos].height": chain[finalizedHeadPos].height,
        "info.finalizedHead.height": info.finalizedHead.height,
        chain,
      });
    }
    // TODO: this assert sometime raises error
    // assert(chain[finalizedHeadPos].hash === info.finalizedHead.hash);

    await this.deleteFinalizedHotBlocks(info.finalizedHead.height);
    await this.updateStatus(info.finalizedHead);
  }

  private async deleteFinalizedHotBlocks(finalizedHeight: number): Promise<void> {
    await this.client.exec(
      `DELETE
       FROM ${this.stateSchema}.hot_block
       ON CLUSTER cluster1
       WHERE height <= ${finalizedHeight}
         AND chain_id = ${this.chainId}`,
      this.debug,
    );
  }

  private async insertHotBlock(block: HashAndHeight): Promise<void> {
    await this.client.insert(`${this.stateSchema}.hot_block`, [{ ...block, chain_id: this.chainId }]);
  }

  private async updateStatus(next: HashAndHeight): Promise<void> {
    await this.client?.insert(`${this.stateSchema}.status`, [
      {
        chain_id: this.chainId,
        height: next.height,
        hash: next.hash,
      },
    ]);
  }

  private async performUpdates(cb: (store: Store) => Promise<void>, changeTracker?: ChangeTracker): Promise<void> {
    const store = new Store(this.client, changeTracker, this.tablesConfig);
    await cb(store);
  }

  async getLastStatus(): Promise<HashAndHeight> {
    const res = await this.client.query<{ hash: string; height: number }>(
      `SELECT hash, height
       FROM ${this.stateSchema}.status FINAL
       WHERE chain_id = ${this.chainId}`,
    );
    return res[0] || { height: -1, hash: "0x" };
  }

  async getHotBlocks(): Promise<HashAndHeight[]> {
    return await this.client.query<{ hash: string; height: number }>(
        `SELECT hash, height
       FROM ${this.stateSchema}.hot_block FINAL
       WHERE chain_id = ${this.chainId}
       ORDER BY height ASC`,
    );
  }

  private async getState(): Promise<DatabaseState> {
    const status = await this.getLastStatus();
    const top = await this.getHotBlocks();
    const res = { ...status, top };
    assertStateInvariants(res);
    return res;
  }
}

const RACE_MSG = "status table was updated by foreign process, make sure no other processor is running";

function assertStateInvariants(state: DatabaseState) {
  const height = state.height;

  // Sanity check. Who knows what driver will return?
  assert(Number.isSafeInteger(height));
  // TODO: this assert sometime raises error
  // assertChainContinuity(state, state.top);
}

function assertChainContinuity(base: HashAndHeight, chain: HashAndHeight[]) {
  let prev = base;
  for (const b of chain) {
    assert(b.height === prev.height + 1, "blocks must form a continues chain");
    prev = b;
  }
}
