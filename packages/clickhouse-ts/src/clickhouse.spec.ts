import { ClickhouseConnection } from "./clickhouse";

describe("clickhouse", () => {
  it("should work", () => {
    const clickhouse = new ClickhouseConnection();
    expect(clickhouse).toBeTruthy();
  });
});
