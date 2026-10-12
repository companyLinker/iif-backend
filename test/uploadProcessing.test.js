import test from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import {
  extractRecords,
  loadKnownColumns,
  storeRecords,
  readUploadedFile,
  withBrandLock,
} from "../uploadProcessing.js";

// Minimal in-memory stand-in for a MongoDB collection.
function fakeCollection(initial = []) {
  const docs = initial.map((d, i) => ({ _id: `seed${i}`, ...d }));
  let nextId = 0;
  const matches = (doc, filter) =>
    Object.entries(filter).every(([k, cond]) =>
      cond && typeof cond === "object" && "$in" in cond
        ? cond.$in.includes(doc[k])
        : doc[k] === cond,
    );
  return {
    docs,
    find(filter = {}) {
      let result = docs.filter((d) => matches(d, filter));
      return {
        limit(n) {
          result = result.slice(0, n);
          return this;
        },
        toArray: async () => result.map((d) => ({ ...d })),
      };
    },
    async insertMany(list) {
      for (const d of list) docs.push({ _id: `new${nextId++}`, ...d });
      return { insertedCount: list.length };
    },
    async bulkWrite(ops) {
      let modified = 0;
      for (const { updateOne } of ops) {
        const doc = docs.find((d) => d._id === updateOne.filter._id);
        Object.assign(doc, updateOne.update.$set);
        modified++;
      }
      return { modifiedCount: modified };
    },
  };
}

const sheet = (rows, sheetName = "Sheet1") => [{ sheetName, rows }];

test("headers in any case/spacing are recognised (the '0 inserted' bug)", async () => {
  for (const [store, date] of [
    ["STORENAME", "DATE"],
    ["Store Name", "Date"],
    ["storename", "date"],
    ["Store", "Business Date"],
    ["Location", "Report Date"],
  ]) {
    const { records, skipped } = extractRecords(
      sheet([
        [store, date, "Net Sales"],
        ["PLK 1", "09/01/2026", 100],
      ]),
    );
    assert.equal(records.length, 1, `${store}/${date}`);
    assert.equal(records[0].StoreName, "PLK 1");
    assert.equal(records[0].Date, "09-01-2026");
    assert.equal(skipped.missingStoreName + skipped.missingOrInvalidDate, 0);
  }
});

test("title rows above the real header are skipped", () => {
  const { records } = extractRecords(
    sheet([
      ["Daily Collection Report", null, null],
      [null, null, null],
      ["Store", "Date", "Sales"],
      ["PLK 1", "09-01-2026", 5],
    ]),
  );
  assert.equal(records.length, 1);
  assert.equal(records[0].Sales, 5);
});

test("rows that cannot be stored are counted with a reason, never silently dropped", () => {
  const { records, skipped } = extractRecords(
    sheet([
      ["StoreName", "Date", "Sales"],
      ["PLK 1", "not a date", 1],
      [null, "09-01-2026", 2],
      ["PLK 2", "09-01-2026", 3],
    ]),
  );
  assert.equal(records.length, 1);
  assert.equal(skipped.missingOrInvalidDate, 1);
  assert.equal(skipped.missingStoreName, 1);
  assert.equal(skipped.samples.length, 2);
});

test("no store/date columns at all is reported, not hidden", () => {
  const { records, skipped, sheetsWithoutHeader } = extractRecords(
    sheet([
      ["Foo", "Bar"],
      ["a", "b"],
    ]),
  );
  assert.equal(records.length, 0);
  assert.equal(skipped.missingStoreName, 1);
  assert.deepEqual(sheetsWithoutHeader, ["Sheet1"]);
});

test("Excel serial dates are converted", () => {
  const { records } = extractRecords(
    sheet([
      ["StoreName", "Date"],
      ["PLK 1", 46266], // 2026-09-01
    ]),
  );
  assert.equal(records[0].Date, "09-01-2026");
});

test("headers matching an existing column reuse its name (no split columns)", async () => {
  const col = fakeCollection([
    { StoreName: "PLK 1", Date: "08-31-2026", NetSalesICR: 1, "Non-TaxableSal": 2 },
  ]);
  const known = await loadKnownColumns(col);
  const { records } = extractRecords(
    sheet([
      ["STORE NAME", "DATE", "Net Sales ICR", "NON TAXABLE SAL", "Brand New"],
      ["PLK 1", "09-01-2026", 10, 20, 30],
    ]),
    known,
  );
  assert.deepEqual(Object.keys(records[0]).sort(), [
    "BrandNew",
    "Date",
    "NetSalesICR",
    "Non-TaxableSal",
    "StoreName",
  ]);
  assert.equal(records[0].NetSalesICR, 10);
});

test("existing column names containing spaces are preserved exactly", async () => {
  const col = fakeCollection([
    { StoreName: "PLK 1", Date: "08-31-2026", "Transaction Count": 5, "Cash Shortage": 1 },
  ]);
  const known = await loadKnownColumns(col);
  const { records } = extractRecords(
    sheet([
      ["Store Name", "Date", "Transaction Count", "CASH SHORTAGE", "Tip Received"],
      ["PLK 1", "09-01-2026", 9, 2, 4],
    ]),
    known,
  );
  assert.deepEqual(Object.keys(records[0]).sort(), [
    "Cash Shortage",
    "Date",
    "StoreName",
    "TipReceived",
    "Transaction Count",
  ]);
  assert.equal(records[0]["Transaction Count"], 9);
});

test("uploading the same data twice stores it once", async () => {
  const col = fakeCollection();
  const rows = [
    ["StoreName", "Date", "Sales"],
    ["PLK 1", "09-01-2026", 1],
    ["PLK 2", "09-01-2026", 2],
  ];
  const first = await storeRecords(col, extractRecords(sheet(rows)).records);
  const second = await storeRecords(col, extractRecords(sheet(rows)).records);
  assert.equal(first.inserted, 2);
  assert.equal(second.inserted, 0);
  assert.equal(second.duplicates, 2);
  assert.equal(col.docs.length, 2);
});

test("partial overlap inserts only the new rows", async () => {
  const col = fakeCollection();
  await storeRecords(
    col,
    extractRecords(
      sheet([
        ["StoreName", "Date", "Sales"],
        ["PLK 1", "09-01-2026", 1],
        ["PLK 2", "09-01-2026", 2],
      ]),
    ).records,
  );
  const res = await storeRecords(
    col,
    extractRecords(
      sheet([
        ["StoreName", "Date", "Sales"],
        ["PLK 2", "09-01-2026", 2],
        ["PLK 3", "09-01-2026", 3],
        ["PLK 1", "09-02-2026", 4],
      ]),
    ).records,
  );
  assert.equal(res.inserted, 2);
  assert.equal(res.duplicates, 1);
  assert.equal(col.docs.length, 4);
});

test("duplicates inside one file, and store-name case/space variants, are collapsed", async () => {
  const col = fakeCollection();
  const res = await storeRecords(
    col,
    extractRecords(
      sheet([
        ["StoreName", "Date", "Sales"],
        ["PLK 1", "09-01-2026", 1],
        ["PLK 1", "09-01-2026", 1],
        ["plk  1", "09-01-2026", 1],
      ]),
    ).records,
  );
  assert.equal(res.inserted, 1);
  assert.equal(res.duplicates, 2);
  assert.equal(col.docs.length, 1);
});

test("re-upload with extra columns fills gaps without overwriting stored values", async () => {
  const col = fakeCollection([
    { StoreName: "PLK 1", Date: "09-01-2026", Sales: 100, Tips: null },
  ]);
  const res = await storeRecords(
    col,
    extractRecords(
      sheet([
        ["StoreName", "Date", "Sales", "Tips", "Refunds"],
        ["PLK 1", "09-01-2026", 999, 7, 3],
      ]),
    ).records,
  );
  assert.equal(res.inserted, 0);
  assert.equal(res.updated, 1);
  assert.equal(col.docs.length, 1);
  assert.equal(col.docs[0].Sales, 100); // existing value kept
  assert.equal(col.docs[0].Tips, 7); // empty value filled
  assert.equal(col.docs[0].Refunds, 3); // new column added
});

test("concurrent uploads of the same data do not duplicate it", async () => {
  const col = fakeCollection();
  const make = () =>
    extractRecords(
      sheet([
        ["StoreName", "Date", "Sales"],
        ["PLK 1", "09-01-2026", 1],
      ]),
    ).records;
  await Promise.all([
    withBrandLock("b", () => storeRecords(col, make())),
    withBrandLock("b", () => storeRecords(col, make())),
    withBrandLock("b", () => storeRecords(col, make())),
  ]);
  assert.equal(col.docs.length, 1);
});

test("CSV files are read with the same header detection", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "upl-"));
  const file = path.join(dir, "x.csv");
  fs.writeFileSync(file, "STORE,DATE,Sales\nPLK 1,09/01/2026,12.5\nPLK 2,09/01/2026,7\n");
  const sheets = await readUploadedFile(file, "csv");
  const { records } = extractRecords(sheets);
  assert.equal(records.length, 2);
  assert.equal(records[0].StoreName, "PLK 1");
  assert.equal(records[0].Date, "09-01-2026");
  assert.equal(records[0].Sales, 12.5);
  fs.rmSync(dir, { recursive: true, force: true });
});
