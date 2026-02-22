// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
// ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
// ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
// ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
// ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
// ┃ This file is part of the Perspective library, distributed under the terms ┃
// ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
// ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

import * as fs from "fs";
import * as path from "path";
import { createRequire } from "module";

import * as duckdb from "@duckdb/duckdb-wasm";

import { test, expect } from "@perspective-dev/test";
import {
    default as perspective,
    createMessageHandler,
    wasmModule,
} from "@perspective-dev/client";
import { DuckDBHandler } from "@perspective-dev/client/src/ts/virtual_servers/duckdb.ts";

const require = createRequire(import.meta.url);
const DUCKDB_DIST = path.dirname(require.resolve("@duckdb/duckdb-wasm"));
const Worker = require("web-worker");

async function initializeDuckDB() {
    const bundle = await duckdb.selectBundle({
        mvp: {
            mainModule: path.resolve(DUCKDB_DIST, "./duckdb-mvp.wasm"),
            mainWorker: path.resolve(
                DUCKDB_DIST,
                "./duckdb-node-mvp.worker.cjs",
            ),
        },
        eh: {
            mainModule: path.resolve(DUCKDB_DIST, "./duckdb-eh.wasm"),
            mainWorker: path.resolve(
                DUCKDB_DIST,
                "./duckdb-node-eh.worker.cjs",
            ),
        },
    });

    const logger = new duckdb.ConsoleLogger();
    const worker = new Worker(bundle.mainWorker);
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    const c = await db.connect();
    await c.query(`
        SET default_null_order=NULLS_FIRST_ON_ASC_LAST_ON_DESC;
    `);

    return c;
}

async function loadSuperstoreData(db) {
    const arrowPath = path.resolve(
        import.meta.dirname,
        "../../node_modules/superstore-arrow/superstore.lz4.arrow",
    );

    const arrayBuffer = fs.readFileSync(arrowPath);
    await db.insertArrowFromIPCStream(new Uint8Array(arrayBuffer), {
        name: "superstore",
        create: true,
    });
}

test.describe("DuckDB Virtual Server", function () {
    let db;
    let client;

    test.beforeAll(async () => {
        db = await initializeDuckDB();
        const server = createMessageHandler(new DuckDBHandler(db, wasmModule));
        client = await perspective.worker(server);
        await loadSuperstoreData(db);
    });

    test.describe("client", () => {
        test("get_hosted_table_names()", async function () {
            const tables = await client.get_hosted_table_names();
            expect(tables).toEqual(["memory.superstore"]);
        });
    });

    test.describe("table", () => {
        test("schema()", async function () {
            const table = await client.open_table("memory.superstore");
            const schema = await table.schema();
            expect(schema).toEqual({
                "Product Name": "string",
                "Ship Date": "date",
                City: "string",
                "Row ID": "integer",
                "Customer Name": "string",
                Quantity: "integer",
                Discount: "float",
                "Sub-Category": "string",
                Segment: "string",
                Category: "string",
                "Order Date": "date",
                "Order ID": "string",
                Sales: "float",
                State: "string",
                "Postal Code": "float",
                Country: "string",
                "Customer ID": "string",
                "Ship Mode": "string",
                Region: "string",
                Profit: "float",
                "Product ID": "string",
            });
        });

        test("columns()", async function () {
            const table = await client.open_table("memory.superstore");
            const columns = await table.columns();
            expect(columns).toEqual([
                "Row ID",
                "Order ID",
                "Order Date",
                "Ship Date",
                "Ship Mode",
                "Customer ID",
                "Customer Name",
                "Segment",
                "Country",
                "City",
                "State",
                "Postal Code",
                "Region",
                "Product ID",
                "Category",
                "Sub-Category",
                "Product Name",
                "Sales",
                "Quantity",
                "Discount",
                "Profit",
            ]);
        });

        test("size()", async function () {
            const table = await client.open_table("memory.superstore");
            const size = await table.size();
            expect(size).toBe(9994);
        });
    });

    test.describe("view", () => {
        test("num_rows()", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({ columns: ["Sales", "Profit"] });
            const numRows = await view.num_rows();
            expect(numRows).toBe(9994);
            await view.delete();
        });

        test("num_columns()", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Profit", "State"],
            });

            const numColumns = await view.num_columns();
            expect(numColumns).toBe(3);
            await view.delete();
        });

        test("schema()", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Profit", "State"],
            });
            const schema = await view.schema();
            expect(schema).toEqual({
                Sales: "float",
                Profit: "float",
                State: "string",
            });
            await view.delete();
        });

        test("to_json()", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Quantity"],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 261.96, Quantity: 2 },
                { Sales: 731.94, Quantity: 3 },
                { Sales: 14.62, Quantity: 2 },
                { Sales: 957.5775, Quantity: 5 },
                { Sales: 22.368, Quantity: 2 },
            ]);
            await view.delete();
        });

        test("to_columns()", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Quantity"],
            });
            const columns = await view.to_columns({
                start_row: 0,
                end_row: 5,
            });
            expect(columns).toEqual({
                Sales: [261.96, 731.94, 14.62, 957.5775, 22.368],
                Quantity: [2, 3, 2, 5, 2],
            });
            await view.delete();
        });

        test("column_paths()", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Profit", "State"],
            });
            const paths = await view.column_paths();
            expect(paths).toEqual(["Sales", "Profit", "State"]);
            await view.delete();
        });
    });

    test.describe("group_by", () => {
        test("single group_by", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                group_by: ["Region"],
                aggregates: { Sales: "sum" },
            });
            const numRows = await view.num_rows();
            expect(numRows).toBe(5);
            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], Sales: 2297200.860299955 },
                {
                    __ROW_PATH__: ["Central"],
                    Sales: 501239.8908000005,
                },
                { __ROW_PATH__: ["East"], Sales: 678781.2399999979 },
                {
                    __ROW_PATH__: ["South"],
                    Sales: 391721.9050000003,
                },
                { __ROW_PATH__: ["West"], Sales: 725457.8245000006 },
            ]);
            await view.delete();
        });

        test("multi-level group_by", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                group_by: ["Region", "Category"],
                aggregates: { Sales: "sum" },
            });
            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], Sales: 2297200.860299955 },
                {
                    __ROW_PATH__: ["Central"],
                    Sales: 501239.8908000005,
                },
                {
                    __ROW_PATH__: ["Central", "Furniture"],
                    Sales: 163797.16380000004,
                },
                {
                    __ROW_PATH__: ["Central", "Office Supplies"],
                    Sales: 167026.41500000027,
                },
                {
                    __ROW_PATH__: ["Central", "Technology"],
                    Sales: 170416.3119999999,
                },
                { __ROW_PATH__: ["East"], Sales: 678781.2399999979 },
                {
                    __ROW_PATH__: ["East", "Furniture"],
                    Sales: 208291.20400000009,
                },
                {
                    __ROW_PATH__: ["East", "Office Supplies"],
                    Sales: 205516.0549999999,
                },
                {
                    __ROW_PATH__: ["East", "Technology"],
                    Sales: 264973.9810000003,
                },
                {
                    __ROW_PATH__: ["South"],
                    Sales: 391721.9050000003,
                },
                {
                    __ROW_PATH__: ["South", "Furniture"],
                    Sales: 117298.6840000001,
                },
                {
                    __ROW_PATH__: ["South", "Office Supplies"],
                    Sales: 125651.31299999992,
                },
                {
                    __ROW_PATH__: ["South", "Technology"],
                    Sales: 148771.9079999999,
                },
                { __ROW_PATH__: ["West"], Sales: 725457.8245000006 },
                {
                    __ROW_PATH__: ["West", "Furniture"],
                    Sales: 252612.7435000003,
                },
                {
                    __ROW_PATH__: ["West", "Office Supplies"],
                    Sales: 220853.24900000007,
                },
                {
                    __ROW_PATH__: ["West", "Technology"],
                    Sales: 251991.83199999997,
                },
            ]);
            await view.delete();
        });

        test("group_by with count aggregate", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                group_by: ["Region"],
                aggregates: { Sales: "count" },
            });
            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], Sales: 9994 },
                { __ROW_PATH__: ["Central"], Sales: 2323 },
                { __ROW_PATH__: ["East"], Sales: 2848 },
                { __ROW_PATH__: ["South"], Sales: 1620 },
                { __ROW_PATH__: ["West"], Sales: 3203 },
            ]);
            await view.delete();
        });

        test("group_by with avg aggregate", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                group_by: ["Category"],
                aggregates: { Sales: "avg" },
            });
            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], Sales: 229.8580008304938 },
                {
                    __ROW_PATH__: ["Furniture"],
                    Sales: 349.83488698727007,
                },
                {
                    __ROW_PATH__: ["Office Supplies"],
                    Sales: 119.32410089611732,
                },
                {
                    __ROW_PATH__: ["Technology"],
                    Sales: 452.70927612344155,
                },
            ]);
            await view.delete();
        });

        test("group_by with min aggregate", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Quantity"],
                group_by: ["Region"],
                aggregates: { Quantity: "min" },
            });
            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], Quantity: 1 },
                { __ROW_PATH__: ["Central"], Quantity: 1 },
                { __ROW_PATH__: ["East"], Quantity: 1 },
                { __ROW_PATH__: ["South"], Quantity: 1 },
                { __ROW_PATH__: ["West"], Quantity: 1 },
            ]);
            await view.delete();
        });

        test("group_by with max aggregate", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Quantity"],
                group_by: ["Region"],
                aggregates: { Quantity: "max" },
            });
            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], Quantity: 14 },
                { __ROW_PATH__: ["Central"], Quantity: 14 },
                { __ROW_PATH__: ["East"], Quantity: 14 },
                { __ROW_PATH__: ["South"], Quantity: 14 },
                { __ROW_PATH__: ["West"], Quantity: 14 },
            ]);
            await view.delete();
        });
    });

    test.describe("split_by", () => {
        test("single split_by", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                split_by: ["Region"],
                group_by: ["Category"],
                aggregates: { Sales: "sum" },
            });

            const columns = await view.column_paths();
            expect(columns).toEqual([
                "Central_Sales",
                "East_Sales",
                "South_Sales",
                "West_Sales",
            ]);

            const json = await view.to_json();
            expect(json).toEqual([
                {
                    __ROW_PATH__: [],
                    "Central|Sales": 501239.8908000005,
                    "East|Sales": 678781.2399999979,
                    "South|Sales": 391721.9050000003,
                    "West|Sales": 725457.8245000006,
                },
                {
                    __ROW_PATH__: ["Furniture"],
                    "Central|Sales": 163797.16380000004,
                    "East|Sales": 208291.20400000009,
                    "South|Sales": 117298.6840000001,
                    "West|Sales": 252612.7435000003,
                },
                {
                    __ROW_PATH__: ["Office Supplies"],
                    "Central|Sales": 167026.41500000027,
                    "East|Sales": 205516.0549999999,
                    "South|Sales": 125651.31299999992,
                    "West|Sales": 220853.24900000007,
                },
                {
                    __ROW_PATH__: ["Technology"],
                    "Central|Sales": 170416.3119999999,
                    "East|Sales": 264973.9810000003,
                    "South|Sales": 148771.9079999999,
                    "West|Sales": 251991.83199999997,
                },
            ]);
            await view.delete();
        });

        test.skip("split_by without group_by", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                split_by: ["Category"],
            });
            const paths = await view.column_paths();
            expect(paths.some((c) => c.includes("Furniture"))).toBe(true);
            expect(paths.some((c) => c.includes("Office Supplies"))).toBe(true);
            expect(paths.some((c) => c.includes("Technology"))).toBe(true);
            await view.delete();
        });
    });

    test.describe("filter", () => {
        test("filter with equals", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Region"],
                filter: [["Region", "==", "West"]],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 14.62, Region: "West" },
                { Sales: 48.86, Region: "West" },
                { Sales: 7.28, Region: "West" },
                { Sales: 907.152, Region: "West" },
                { Sales: 18.504, Region: "West" },
            ]);
            await view.delete();
        });

        test("filter with not equals", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Region"],
                filter: [["Region", "!=", "West"]],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 261.96, Region: "South" },
                { Sales: 731.94, Region: "South" },
                { Sales: 957.5775, Region: "South" },
                { Sales: 22.368, Region: "South" },
                { Sales: 15.552, Region: "South" },
            ]);
            await view.delete();
        });

        test("filter with greater than", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Quantity"],
                filter: [["Quantity", ">", 5]],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 48.86, Quantity: 7 },
                { Sales: 907.152, Quantity: 6 },
                { Sales: 1706.184, Quantity: 9 },
                { Sales: 665.88, Quantity: 6 },
                { Sales: 19.46, Quantity: 7 },
            ]);
            await view.delete();
        });

        test("filter with less than", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Quantity"],
                filter: [["Quantity", "<", 3]],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 261.96, Quantity: 2 },
                { Sales: 14.62, Quantity: 2 },
                { Sales: 22.368, Quantity: 2 },
                { Sales: 55.5, Quantity: 2 },
                { Sales: 8.56, Quantity: 2 },
            ]);
            await view.delete();
        });

        test("filter with greater than or equal", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Quantity"],
                filter: [["Quantity", ">=", 10]],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 40.096, Quantity: 14 },
                { Sales: 43.12, Quantity: 14 },
                { Sales: 384.45, Quantity: 11 },
                { Sales: 3347.37, Quantity: 13 },
                { Sales: 100.24, Quantity: 10 },
            ]);
            await view.delete();
        });

        test("filter with less than or equal", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Quantity"],
                filter: [["Quantity", "<=", 2]],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 261.96, Quantity: 2 },
                { Sales: 14.62, Quantity: 2 },
                { Sales: 22.368, Quantity: 2 },
                { Sales: 55.5, Quantity: 2 },
                { Sales: 8.56, Quantity: 2 },
            ]);
            await view.delete();
        });

        test("filter with LIKE", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "State"],
                filter: [["State", "LIKE", "Cal%"]],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 14.62, State: "California" },
                { Sales: 48.86, State: "California" },
                { Sales: 7.28, State: "California" },
                { Sales: 907.152, State: "California" },
                { Sales: 18.504, State: "California" },
            ]);
            await view.delete();
        });

        test("multiple filters", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Region", "Quantity"],
                filter: [
                    ["Region", "==", "West"],
                    ["Quantity", ">", 3],
                ],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 48.86, Region: "West", Quantity: 7 },
                { Sales: 7.28, Region: "West", Quantity: 4 },
                { Sales: 907.152, Region: "West", Quantity: 6 },
                { Sales: 114.9, Region: "West", Quantity: 5 },
                { Sales: 1706.184, Region: "West", Quantity: 9 },
            ]);
            await view.delete();
        });

        test("filter with group_by", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                group_by: ["Category"],
                filter: [["Region", "==", "West"]],
                aggregates: { Sales: "sum" },
            });
            const numRows = await view.num_rows();
            expect(numRows).toBe(4);
            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], Sales: 725457.8245000006 },
                {
                    __ROW_PATH__: ["Furniture"],
                    Sales: 252612.7435000003,
                },
                {
                    __ROW_PATH__: ["Office Supplies"],
                    Sales: 220853.24900000007,
                },
                {
                    __ROW_PATH__: ["Technology"],
                    Sales: 251991.83199999997,
                },
            ]);
            await view.delete();
        });
    });

    test.describe("sort", () => {
        test("sort ascending", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Quantity"],
                sort: [["Sales", "asc"]],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 0.444, Quantity: 1 },
                { Sales: 0.556, Quantity: 1 },
                { Sales: 0.836, Quantity: 1 },
                { Sales: 0.852, Quantity: 1 },
                { Sales: 0.876, Quantity: 1 },
            ]);
            await view.delete();
        });

        test("sort descending", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Quantity"],
                sort: [["Sales", "desc"]],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 22638.48, Quantity: 6 },
                { Sales: 17499.95, Quantity: 5 },
                { Sales: 13999.96, Quantity: 4 },
                { Sales: 11199.968, Quantity: 4 },
                { Sales: 10499.97, Quantity: 3 },
            ]);
            await view.delete();
        });

        test("sort with group_by", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                group_by: ["Region"],
                sort: [["Sales", "desc"]],
                aggregates: { Sales: "sum" },
            });
            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], Sales: 2297200.860299955 },
                { __ROW_PATH__: ["West"], Sales: 725457.8245000006 },
                { __ROW_PATH__: ["East"], Sales: 678781.2399999979 },
                {
                    __ROW_PATH__: ["Central"],
                    Sales: 501239.8908000005,
                },
                {
                    __ROW_PATH__: ["South"],
                    Sales: 391721.9050000003,
                },
            ]);
            await view.delete();
        });

        test("multi-column sort", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Region", "Sales", "Quantity"],
                sort: [
                    ["Region", "asc"],
                    ["Sales", "desc"],
                ],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Region: "Central", Sales: 17499.95, Quantity: 5 },
                { Region: "Central", Sales: 9892.74, Quantity: 13 },
                { Region: "Central", Sales: 9449.95, Quantity: 5 },
                { Region: "Central", Sales: 8159.952, Quantity: 8 },
                { Region: "Central", Sales: 5443.96, Quantity: 4 },
            ]);
            await view.delete();
        });
    });

    test.describe("expressions", () => {
        test("simple expression", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "doublesales"],
                expressions: { doublesales: '"Sales" * 2' },
            });

            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 261.96, doublesales: 523.92 },
                { Sales: 731.94, doublesales: 1463.88 },
                { Sales: 14.62, doublesales: 29.24 },
                { Sales: 957.5775, doublesales: 1915.155 },
                { Sales: 22.368, doublesales: 44.736 },
            ]);

            await view.delete();
        });

        test("expression with multiple columns", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Profit", "margin"],
                expressions: { margin: '"Profit" / "Sales"' },
            });

            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                {
                    Sales: 261.96,
                    Profit: 41.9136,
                    margin: 0.16000000000000003,
                },
                { Sales: 731.94, Profit: 219.582, margin: 0.3 },
                {
                    Sales: 14.62,
                    Profit: 6.8714,
                    margin: 0.47000000000000003,
                },
                { Sales: 957.5775, Profit: -383.031, margin: -0.4 },
                { Sales: 22.368, Profit: 2.5164, margin: 0.1125 },
            ]);

            await view.delete();
        });

        test("expression with group_by", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["total"],
                group_by: ["Region"],
                expressions: { total: '"Sales" + "Profit"' },
                aggregates: { total: "sum" },
            });

            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], total: 2583597.882000014 },
                {
                    __ROW_PATH__: ["Central"],
                    total: 540946.2532999996,
                },
                { __ROW_PATH__: ["East"], total: 770304.0199999991 },
                {
                    __ROW_PATH__: ["South"],
                    total: 438471.33530000027,
                },
                { __ROW_PATH__: ["West"], total: 833876.2733999988 },
            ]);

            await view.delete();
        });
    });

    test.describe("viewport", () => {
        test("start_row and end_row", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Profit"],
            });
            const json = await view.to_json({ start_row: 10, end_row: 15 });
            expect(json).toEqual([
                { Sales: 1706.184, Profit: 85.3092 },
                { Sales: 911.424, Profit: 68.3568 },
                { Sales: 15.552, Profit: 5.4432 },
                { Sales: 407.976, Profit: 132.5922 },
                { Sales: 68.81, Profit: -123.858 },
            ]);
            await view.delete();
        });

        test("start_col and end_col", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Profit", "Quantity", "Discount"],
            });
            const json = await view.to_json({
                start_row: 0,
                end_row: 5,
                start_col: 1,
                end_col: 3,
            });
            expect(json).toEqual([
                { Profit: 41.9136, Quantity: 2 },
                { Profit: 219.582, Quantity: 3 },
                { Profit: 6.8714, Quantity: 2 },
                { Profit: -383.031, Quantity: 5 },
                { Profit: 2.5164, Quantity: 2 },
            ]);
            await view.delete();
        });
    });

    test.describe("data types", () => {
        test("integer columns", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Quantity"],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Quantity: 2 },
                { Quantity: 3 },
                { Quantity: 2 },
                { Quantity: 5 },
                { Quantity: 2 },
            ]);
            await view.delete();
        });

        test("float columns", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales", "Profit"],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { Sales: 261.96, Profit: 41.9136 },
                { Sales: 731.94, Profit: 219.582 },
                { Sales: 14.62, Profit: 6.8714 },
                { Sales: 957.5775, Profit: -383.031 },
                { Sales: 22.368, Profit: 2.5164 },
            ]);
            await view.delete();
        });

        test("string columns", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Region", "State", "City"],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                {
                    Region: "South",
                    State: "Kentucky",
                    City: "Henderson",
                },
                {
                    Region: "South",
                    State: "Kentucky",
                    City: "Henderson",
                },
                {
                    Region: "West",
                    State: "California",
                    City: "Los Angeles",
                },
                {
                    Region: "South",
                    State: "Florida",
                    City: "Fort Lauderdale",
                },
                {
                    Region: "South",
                    State: "Florida",
                    City: "Fort Lauderdale",
                },
            ]);
            await view.delete();
        });

        test("date columns", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Order Date"],
            });
            const json = await view.to_json({ start_row: 0, end_row: 5 });
            expect(json).toEqual([
                { "Order Date": 1478563200000 },
                { "Order Date": 1478563200000 },
                { "Order Date": 1465689600000 },
                { "Order Date": 1444521600000 },
                { "Order Date": 1444521600000 },
            ]);
            await view.delete();
        });
    });

    test.describe("combined operations", () => {
        test("group_by + filter + sort", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                group_by: ["Category"],
                filter: [["Region", "==", "West"]],
                sort: [["Sales", "desc"]],
                aggregates: { Sales: "sum" },
            });
            const json = await view.to_json();
            expect(json).toEqual([
                { __ROW_PATH__: [], Sales: 725457.8245000006 },
                {
                    __ROW_PATH__: ["Furniture"],
                    Sales: 252612.7435000003,
                },
                {
                    __ROW_PATH__: ["Technology"],
                    Sales: 251991.83199999997,
                },
                {
                    __ROW_PATH__: ["Office Supplies"],
                    Sales: 220853.24900000007,
                },
            ]);
            await view.delete();
        });

        test("split_by + group_by + filter", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                group_by: ["Category"],
                split_by: ["Region"],
                filter: [["Quantity", ">", 3]],
                aggregates: { Sales: "sum" },
            });

            const paths = await view.column_paths();
            expect(paths).toEqual([
                "Central_Sales",
                "East_Sales",
                "South_Sales",
                "West_Sales",
            ]);

            const numRows = await view.num_rows();
            expect(numRows).toBe(4);

            const json = await view.to_json();
            expect(json).toEqual([
                {
                    __ROW_PATH__: [],
                    "Central|Sales": 332883.0567999998,
                    "East|Sales": 455143.735,
                    "South|Sales": 274208.7699999999,
                    "West|Sales": 470561.28350000136,
                },
                {
                    __ROW_PATH__: ["Furniture"],
                    "Central|Sales": 111457.73279999988,
                    "East|Sales": 140376.95899999997,
                    "South|Sales": 80859.618,
                    "West|Sales": 165219.5734999998,
                },
                {
                    __ROW_PATH__: ["Office Supplies"],
                    "Central|Sales": 103937.78599999992,
                    "East|Sales": 135823.893,
                    "South|Sales": 84393.3579999999,
                    "West|Sales": 140206.93099999975,
                },
                {
                    __ROW_PATH__: ["Technology"],
                    "Central|Sales": 117487.53800000002,
                    "East|Sales": 178942.883,
                    "South|Sales": 108955.79400000005,
                    "West|Sales": 165134.77900000007,
                },
            ]);
            await view.delete();
        });

        test("split_by only", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["Sales"],
                split_by: ["Region"],
                filter: [["Quantity", ">", 3]],
            });

            const paths = await view.column_paths();
            expect(paths).toEqual([
                "Central_Sales",
                "East_Sales",
                "South_Sales",
                "West_Sales",
            ]);

            const numRows = await view.num_rows();
            expect(numRows).toBe(4284);
            const json = await view.to_json({ start_row: 0, end_row: 1 });
            expect(json).toEqual([
                {
                    "Central|Sales": null,
                    "East|Sales": null,
                    "South|Sales": 957.5775,
                    "West|Sales": null,
                },
            ]);
            await view.delete();
        });

        test("expressions + group_by + sort", async function () {
            const table = await client.open_table("memory.superstore");
            const view = await table.view({
                columns: ["profitmargin"],
                group_by: ["Region"],
                expressions: { profitmargin: '"Profit" / "Sales" * 100' },
                sort: [["profitmargin", "desc"]],
                aggregates: { profitmargin: "avg" },
            });
            const json = await view.to_json();
            expect(json).toEqual([
                {
                    __ROW_PATH__: [],
                    profitmargin: 12.031392972104467,
                },
                {
                    __ROW_PATH__: ["West"],
                    profitmargin: 21.948661793784012,
                },
                {
                    __ROW_PATH__: ["East"],
                    profitmargin: 16.722695960406636,
                },
                {
                    __ROW_PATH__: ["South"],
                    profitmargin: 16.35190329218107,
                },
                {
                    __ROW_PATH__: ["Central"],
                    profitmargin: -10.407293926323575,
                },
            ]);
            await view.delete();
        });
    });
});
