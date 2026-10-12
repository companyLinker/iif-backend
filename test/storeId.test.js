import test from "node:test";
import assert from "node:assert/strict";
import {
  extractRecords,
  loadKnownColumns,
  loadStoreIdMap,
  storeRecords,
  normId,
} from "../uploadProcessing.js";

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
      for (const { updateOne } of ops) {
        Object.assign(docs.find((d) => d._id === updateOne.filter._id), updateOne.update.$set);
      }
      return { modifiedCount: ops.length };
    },
  };
}

const sheet = (rows) => [{ sheetName: "Sheet1", rows }];

// StoreData looks like { NO: "Burger Exp 1", NAME: "Abe-Clifton" } in production.
const storeData = () =>
  fakeCollection([
    { NO: "Burger Exp 1", NAME: "Abe-Clifton" },
    { NO: "2384", NAME: "#02384 - Vincennes" },
    { NO: "10363", NAME: "PLK 10363" },
  ]);

const abeFile = () =>
  sheet([
    ["StoreNumber", "Date", "Transaction Count", "Taxable Sales", "Cash Shortage"],
    ["Burger Exp 1", "03/01/2026", 120, 1500.5, 0],
    ["Burger Exp 1", "03/02/2026", 98, 1210, 2],
    ["Burger Exp 1", "03/03/2026", 133, 1710.25, 0],
  ]);

test("normId treats 02384, #2384, 2384.0 and ' 2384 ' alike", () => {
  for (const v of ["02384", "#2384", "2384.0", " 2384 ", 2384]) {
    assert.equal(normId(v), "2384");
  }
  assert.equal(normId("  Burger   EXP 1 "), "burger exp 1");
});

test("a file with only a StoreNumber column gets its store name from StoreData", async () => {
  const idToName = await loadStoreIdMap(fakeCollection(), storeData());
  const { records, skipped, storeIds } = extractRecords(abeFile(), new Map(), idToName);

  assert.equal(records.length, 3);
  assert.equal(skipped.missingStoreName, 0);
  assert.equal(records[0].StoreName, "Abe-Clifton");
  assert.equal(records[0].StoreNumber, "Burger Exp 1"); // original id kept
  assert.equal(records[0].Date, "03-01-2026");
  assert.equal(storeIds.resolved, 3);
  assert.equal(storeIds.unresolved.size, 0);
  assert.equal(storeIds.column, "StoreNumber");
});

test("numeric ids resolve whatever their formatting", async () => {
  const idToName = await loadStoreIdMap(fakeCollection(), storeData());
  const { records } = extractRecords(
    sheet([
      ["Store No.", "Date"],
      [2384, "09-01-2026"],
      ["02384", "09-02-2026"],
      ["#10363", "09-01-2026"],
    ]),
    new Map(),
    idToName,
  );
  assert.deepEqual(
    records.map((r) => r.StoreName),
    ["#02384 - Vincennes", "#02384 - Vincennes", "PLK 10363"],
  );
});

test("ids missing from StoreData are still stored (under the id) and reported", async () => {
  const idToName = await loadStoreIdMap(fakeCollection(), storeData());
  const col = fakeCollection();
  const { records, storeIds } = extractRecords(
    sheet([
      ["StoreNumber", "Date", "Sales"],
      ["Brand New Store 9", "09-01-2026", 5],
      ["Burger Exp 1", "09-01-2026", 6],
    ]),
    new Map(),
    idToName,
  );
  const res = await storeRecords(col, records);
  assert.equal(res.inserted, 2);
  assert.deepEqual([...storeIds.unresolved], ["Brand New Store 9"]);
  assert.equal(col.docs.find((d) => d.Sales === 5).StoreName, "Brand New Store 9");
});

test("re-uploading the same id-only file does not duplicate rows", async () => {
  const col = fakeCollection();
  const idToName = await loadStoreIdMap(col, storeData());
  const first = await storeRecords(col, extractRecords(abeFile(), new Map(), idToName).records);
  const known = await loadKnownColumns(col);
  const idToName2 = await loadStoreIdMap(col, storeData());
  const second = await storeRecords(col, extractRecords(abeFile(), known, idToName2).records);

  assert.equal(first.inserted, 3);
  assert.equal(second.inserted, 0);
  assert.equal(second.duplicates, 3);
  assert.equal(col.docs.length, 3);
});

test("a later upload that names the store heals rows stored under the raw id", async () => {
  const col = fakeCollection();
  // first upload: StoreData does not know the store yet
  await storeRecords(col, extractRecords(abeFile(), new Map(), new Map()).records);
  assert.equal(col.docs[0].StoreName, "Burger Exp 1");

  // StoreData later learns it; re-uploading renames instead of duplicating
  const idToName = await loadStoreIdMap(col, storeData());
  const known = await loadKnownColumns(col);
  const res = await storeRecords(col, extractRecords(abeFile(), known, idToName).records);

  assert.equal(res.inserted, 0);
  assert.equal(col.docs.length, 3);
  assert.ok(col.docs.every((d) => d.StoreName === "Abe-Clifton"));
});

test("id duplicates are caught even if the store name differs", async () => {
  const col = fakeCollection([
    { StoreName: "Some Old Name", StoreNo: "2384", Date: "09-01-2026", Sales: 1 },
  ]);
  const idToName = await loadStoreIdMap(col, storeData());
  const { records } = extractRecords(
    sheet([
      ["Store Number", "Date", "Sales"],
      ["2384", "09-01-2026", 99],
    ]),
    await loadKnownColumns(col),
    idToName,
  );
  const res = await storeRecords(col, records);
  assert.equal(res.inserted, 0);
  assert.equal(res.duplicates, 1);
  assert.equal(col.docs.length, 1);
  assert.equal(col.docs[0].Sales, 1); // stored value untouched
});

test("a column called Store holding known ids is treated as an id column", async () => {
  const idToName = await loadStoreIdMap(fakeCollection(), storeData());
  const { records } = extractRecords(
    sheet([
      ["Store", "Date", "Sales"],
      ["2384", "09-01-2026", 1],
      ["10363", "09-01-2026", 2],
    ]),
    new Map(),
    idToName,
  );
  assert.deepEqual(
    records.map((r) => r.StoreName),
    ["#02384 - Vincennes", "PLK 10363"],
  );
});

test("a Store column holding real names is still a name column", async () => {
  const idToName = await loadStoreIdMap(fakeCollection(), storeData());
  const { records } = extractRecords(
    sheet([
      ["Store", "Date"],
      ["Abe-Clifton", "09-01-2026"],
    ]),
    new Map(),
    idToName,
  );
  assert.equal(records[0].StoreName, "Abe-Clifton");
});

test("existing store names in the brand collection take priority over StoreData", async () => {
  const col = fakeCollection([
    { StoreName: "POPEYES # 2384", Date: "08-01-2026" },
  ]);
  const idToName = await loadStoreIdMap(col, storeData());
  assert.equal(idToName.get("2384"), "POPEYES # 2384");
});
