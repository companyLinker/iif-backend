import test from "node:test";
import assert from "node:assert/strict";
import { extractRecords, detectHeaderRowByContent } from "../uploadProcessing.js";

const sheet = (rows) => [{ sheetName: "Sheet1", rows }];
const ids = new Map([
  ["2384", "#02384 - Vincennes"],
  ["10363", "PLK 10363"],
  ["burger exp 1", "Abe-Clifton"],
]);

// 2026-09-01 / 2026-09-02 as Excel serial numbers
const D1 = 46266;
const D2 = 46267;

test("the date column is found whatever its header says", () => {
  for (const header of [
    "Business Day",
    "Report Period",
    "Sale Dt",
    "Txn Date/Time",
    "Posting",
    "Day",
    "When",
    "Column1",
    "Col A",
    "",
    "DATE OF BUSINESS",
    "Fecha",
  ]) {
    const { records, skipped } = extractRecords(
      sheet([
        ["StoreName", header, "Sales"],
        ["PLK 1", D1, 10.5],
        ["PLK 1", D2, 11.5],
        ["PLK 2", D1, 12.5],
      ]),
    );
    assert.equal(records.length, 3, `header "${header}"`);
    assert.equal(records[0].Date, "09-01-2026", `header "${header}"`);
    assert.equal(records[1].Date, "09-02-2026", `header "${header}"`);
    assert.equal(skipped.missingOrInvalidDate, 0);
  }
});

test("date text in any common format is understood", () => {
  const values = [
    "09/01/2026",
    "9/1/2026",
    "2026-09-01",
    "09-01-2026",
    "20260901",
    "Sep 1, 2026",
    "01-Sep-26",
    "2026-09-01 00:00:00",
    "9/1/26",
  ];
  for (const v of values) {
    const { records } = extractRecords(
      sheet([
        ["Store", "Reported On", "Sales"],
        ["PLK 1", v, 1],
        ["PLK 2", v, 2],
      ]),
    );
    assert.equal(records.length, 2, v);
    assert.equal(records[0].Date, "09-01-2026", v);
  }
});

test("YYYYMMDD numbers and text serials from CSV exports become dates", () => {
  const a = extractRecords(
    sheet([
      ["Store", "Posting"],
      ["PLK 1", 20260901],
      ["PLK 2", 20260901],
    ]),
  );
  assert.equal(a.records[0].Date, "09-01-2026");

  const b = extractRecords(
    sheet([
      ["Store", "Business Day"],
      ["PLK 1", "46266"],
      ["PLK 2", "46266"],
    ]),
  );
  assert.equal(b.records[0].Date, "09-01-2026");
});

test("an obvious Date header still wins over another date-looking column", () => {
  const { records } = extractRecords(
    sheet([
      ["StoreName", "Created", "DATE", "Sales"],
      ["PLK 1", "01/01/2020", "09/01/2026", 5],
      ["PLK 2", "01/01/2020", "09/01/2026", 6],
    ]),
  );
  assert.equal(records[0].Date, "09-01-2026");
  assert.equal(records[0].Created, "01/01/2020"); // kept as its own column
});

test("money columns are not mistaken for the date column", () => {
  const { records } = extractRecords(
    sheet([
      ["StoreName", "Net Sales", "Posting"],
      ["PLK 1", 45123.55, D1],
      ["PLK 2", 44890.1, D1],
      ["PLK 3", 46000.9, D1],
    ]),
  );
  assert.equal(records.length, 3);
  assert.equal(records[0].Date, "09-01-2026");
  assert.equal(records[0]["Net Sales"], 45123.55);
});

test("header row is found below title rows even with an unrecognisable date header", () => {
  const rows = [
    ["Abe's Burger", null, null],
    ["Daily report", "run 10/04/2026", null],
    [null, null, null],
    ["Location", "Posting", "Net Sales"],
    ["PLK 1", D1, 100],
    ["PLK 2", D1, 200],
  ];
  assert.equal(detectHeaderRowByContent(rows), 3);
  const { records } = extractRecords(sheet(rows));
  assert.equal(records.length, 2);
  assert.equal(records[0].StoreName, "PLK 1");
  assert.equal(records[0].Date, "09-01-2026");
});

test("a store column with an unexpected header is found by its values", () => {
  const { records, storeIds } = extractRecords(
    sheet([
      ["Posting", "Unit", "Net Sales"],
      [D1, "2384", 1],
      [D1, "10363", 2],
      [D2, "burger exp 1", 3],
    ]),
    new Map(),
    ids,
  );
  assert.deepEqual(
    records.map((r) => r.StoreName),
    ["#02384 - Vincennes", "PLK 10363", "Abe-Clifton"],
  );
  assert.equal(storeIds.resolved, 3);
});

test("a store name column with an unexpected header is found by its values", () => {
  const { records } = extractRecords(
    sheet([
      ["Posting", "Outlet", "Net Sales"],
      [D1, "Abe-Clifton", 1],
      [D2, "PLK 10363", 2],
    ]),
    new Map(),
    ids,
  );
  assert.deepEqual(
    records.map((r) => r.StoreName),
    ["Abe-Clifton", "PLK 10363"],
  );
});

test("headers starting with store/location are used even with extra words", () => {
  const { records } = extractRecords(
    sheet([
      ["Posting", "Store Location Description", "Net Sales"],
      [D1, "PLK 1", 1],
      [D1, "PLK 2", 2],
    ]),
  );
  assert.equal(records[0].StoreName, "PLK 1");
});

test("with a date but no recognisable store, the most store-like text column is used and flagged", () => {
  const sheetRows = [["Posting Day", "Outlet", "Region", "Net Receipts"]];
  for (let i = 0; i < 40; i++) sheetRows.push([`09/${String((i % 28) + 1).padStart(2, "0")}/2026`, `Place ${i}`, i % 2 ? "East" : "West", i * 2]);
  const out = extractRecords(sheet(sheetRows));
  assert.equal(out.records.length, 40);
  assert.equal(out.records[0].StoreName, "Place 0"); // 40 distinct places beat 2 regions
  assert.equal(out.storeIds.guessedColumn, "Outlet");
  assert.equal(out.skipped.missingStoreName, 0);
});

test("a numeric measure is never mistaken for the store, even when small numbers are 'known ids'", () => {
  // what loadStoreIdMap builds from rows already stored: store names, plus ids
  const known = new Map();
  for (let i = 0; i < 500; i++) known.set(String(i), `#${String(i).padStart(5, "0")} - Place ${i}`);
  for (let i = 0; i < 500; i++) known.set(`#${i}`, `#${String(i).padStart(5, "0")} - Place ${i}`);
  const rows = [["Outlet", "Posting Day", "Net Receipts", "Voids", "Tips"]];
  for (let i = 0; i < 300; i++) rows.push([`#${String(i % 500).padStart(5, "0")} - Place ${i % 500}`, "1/5/2026", i * 1.25, i % 9, i % 5]);
  const out = extractRecords(sheet(rows), new Map(), known);
  assert.equal(out.records[7].StoreName, "#00007 - Place 7"); // from Outlet, not from Voids
  assert.equal(out.storeIds.resolved, 0); // no id lookup happened
  assert.equal(out.records[7].Voids, 7 % 9);
});

test("no guess is made without a date, or when no column looks like a store", () => {
  const noDate = extractRecords(sheet([["Alpha", "Beta"], ["x", "y"], ["z", "w"]]));
  assert.equal(noDate.storeIds.guessedColumn, null);
  const numbersOnly = extractRecords(sheet([["Posting Day", "A", "B"], ["09/01/2026", 1, 2], ["09/02/2026", 3, 4], ["09/03/2026", 5, 6]]));
  assert.equal(numbersOnly.records.length, 0);
  assert.equal(numbersOnly.storeIds.guessedColumn, null);
});

test("nothing recognisable is still reported clearly, never silently dropped", () => {
  const { records, skipped } = extractRecords(
    sheet([
      ["Alpha", "Beta"],
      ["x", "y"],
      ["z", "w"],
    ]),
  );
  assert.equal(records.length, 0);
  assert.equal(skipped.missingStoreName, 2);
});
