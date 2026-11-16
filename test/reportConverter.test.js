import test from "node:test";
import assert from "node:assert/strict";
import fs from "fs";
import os from "os";
import path from "path";
import XLSX from "xlsx";
import {
  applyProfile,
  detectMetricsByDate,
  learnProfile,
  matchProfile,
  readMetricSeries,
  transposeReport,
} from "../reportConverter.js";
import { extractRecords } from "../uploadProcessing.js";
import { processUploadedFile } from "../uploadPipeline.js";
import { cleanCell, cleanObjectForSampleData } from "../cellClean.js";
import { makeReport, makeTarget, mdY, sampleMetrics, week } from "./reportFixtures.js";

const days = week("2026-08-05");
const m = sampleMetrics();
const idToName = new Map([["4582", "JJS001"], ["9999", "JJS002"]]);

const reportRows = (extra = {}) => makeReport({ days, metrics: m.list, ...extra });

// the daily summary the sample report should turn into
const targetColumns = () => [
  { name: "Net sales", values: m.royalty },
  { name: "Tax", values: m.tax },
  { name: "VISA", values: m.inShop.map((x, i) => Number((x + m.online[i]).toFixed(2))) },
  { name: "American Express", values: m.amexIn.map((x, i) => Number((x + m.amexOnline[i]).toFixed(2))) },
  { name: "UberEats", values: m.uber },
  { name: "Gift Card" },
  { name: "#1", values: m.deposit },
  { name: "#2", blank: true },
];
const targetRows = () => makeTarget({ storeName: "JJS001", days, columns: targetColumns() });

test("a report (metrics down, days across) is recognised and a flat table is not", () => {
  const layout = detectMetricsByDate(reportRows());
  assert.ok(layout);
  assert.equal(layout.groups.length, 7);
  assert.equal(layout.groups[0].date, "08-05-2026");
  assert.equal(layout.meta["Store #"], "4582");
  assert.equal(layout.title, "Weekly Sales Report (V9)");

  const flat = [
    ["Store Name", "Date", "Created", "Updated", "Sales"],
    ["PLK 1", "09/01/2026", "01/01/2026", "02/01/2026", 10],
    ["PLK 2", "09/01/2026", "01/01/2026", "02/01/2026", 20],
    ["PLK 3", "09/02/2026", "01/01/2026", "02/01/2026", 30],
  ];
  assert.equal(detectMetricsByDate(flat), null);
});

test("AM and PM columns are added per day, text numbers with commas are read", () => {
  const rows = makeReport({
    days,
    metrics: [
      { label: "Big Sales", values: [1234.5, 2000, 3000, 4000, 5000, 6000, 7000] },
      { label: "PM Only", values: [1, 2, 3, 4, 5, 6, 7], pm: true },
    ],
  });
  const parsed = readMetricSeries(detectMetricsByDate(rows), rows);
  assert.deepEqual(parsed.byKey.get("bigsales").values.slice(0, 2), [1234.5, 2000]);
  assert.deepEqual(parsed.byKey.get("pmonly").values, [1, 2, 3, 4, 5, 6, 7]);
  assert.equal(parsed.byKey.has("shiftmanagername"), false); // text-only rows are ignored
});

test("a profile is learned from a sample report and a sample output", () => {
  const { profile, report } = learnProfile({
    sources: [{ rows: reportRows() }],
    targetRows: targetRows(),
    brand: "BrandX",
    name: "Weekly report",
    idToName,
  });
  const col = (name) => profile.columns.find((c) => c.name === name);

  assert.deepEqual(col("Net sales").add, ["= Royalty Sales"]);
  // "Tax" equals both the tax row and a decoy; the name decides
  assert.deepEqual(col("Tax").add, ["+ Sales Tax"]);
  // VISA = InShop + Online, and the all-zero MOTO sibling is included so it is not lost later
  assert.deepEqual(new Set(col("VISA").add), new Set([
    "- A/R Due CC (InShop) V/MC/D",
    "- A/R Due CC (ONLINE) V/MC/D",
    "- A/R Due CC (MOTO) V/MC/D",
  ]));
  assert.equal(col("American Express").add.length, 3);
  assert.deepEqual(col("UberEats").add, ["- UberEats"]);
  assert.deepEqual(col("#1").add, ["= Expected Deposit"]);
  assert.equal(col("#2").blank, true);
  // nothing in the sample can tell which row "Gift Card" is: left at 0, not guessed
  assert.equal(col("Gift Card").constant, 0);
  const gift = report.columns.find((c) => c.column === "Gift Card");
  assert.ok(gift.candidates.length >= 2);

  assert.equal(profile.store.metaKey, "Store #");
  assert.equal(report.store.resolved, true);
  assert.deepEqual(profile.brands, ["BrandX"]);
});

test("the learned profile converts another week of another store", () => {
  const { profile } = learnProfile({
    sources: [{ rows: reportRows() }],
    targetRows: targetRows(),
    brand: "BrandX",
    name: "Weekly report",
    idToName,
  });

  const otherDays = week("2026-09-02");
  const other = makeReport({
    store: "9999",
    location: "Elsewhere",
    days: otherDays,
    metrics: m.list.map((x) => ({ ...x, values: x.values.map((n) => Number((n * 1.5).toFixed(2))) })),
  });
  const parsed = readMetricSeries(detectMetricsByDate(other), other);
  assert.ok(matchProfile(parsed, [profile], "brandx")); // brand match ignores case

  const out = applyProfile(parsed, profile, idToName);
  assert.equal(out.records.length, 7);
  assert.equal(out.records[0].StoreName, "JJS002"); // from the report's own store number
  assert.equal(out.records[0].Date, "09-02-2026");
  assert.equal(out.records[0]["Net sales"], Number((m.royalty[0] * 1.5).toFixed(2)));
  assert.equal(out.records[0].Tax, Number((m.tax[0] * 1.5).toFixed(2)));
  assert.equal(out.records[0]["#2"], null);
  assert.equal(out.records[0]["Gift Card"], 0);
  assert.deepEqual(out.missingLabels, []);
  // the output has exactly the sample's columns, in the sample's order
  assert.deepEqual(Object.keys(out.records[0]), ["StoreName", "Date", ...targetColumns().map((c) => c.name)]);
});

test("rows that add up alike by coincidence: the related rows win over an unrelated pair", () => {
  // straight-line numbers make IN-Originals + UberEats equal InShop + Online
  const line = (a, b) => Array.from({ length: 7 }, (_, i) => a + i * b);
  const metrics = [
    { label: "IN-Originals", values: line(90, 3) },
    { label: "- UberEats", values: line(30, 3) },
    { label: "- A/R Due CC (InShop) V/MC/D", values: line(80, 3) },
    { label: "- A/R Due CC (ONLINE) V/MC/D", values: line(40, 3) },
  ];
  const { profile } = learnProfile({
    sources: [{ rows: makeReport({ days, metrics }) }],
    targetRows: makeTarget({ storeName: "JJS001", days, columns: [{ name: "VISA", values: line(120, 6) }] }),
    brand: "B", name: "n", idToName,
  });
  assert.deepEqual(new Set(profile.columns[0].add), new Set(["- A/R Due CC (InShop) V/MC/D", "- A/R Due CC (ONLINE) V/MC/D"]));
});

test("a store missing from Store Data is kept under its number and reported", () => {
  const { profile } = learnProfile({ sources: [{ rows: reportRows() }], targetRows: targetRows(), brand: "BrandX", name: "n", idToName });
  const other = makeReport({ store: "7777", days, metrics: m.list });
  const parsed = readMetricSeries(detectMetricsByDate(other), other);
  const out = applyProfile(parsed, profile, idToName);
  assert.equal(out.records[0].StoreName, "7777");
  assert.equal(out.store.resolved, false);
});

test("profiles are scoped to their brand and need enough matching rows", () => {
  const { profile } = learnProfile({ sources: [{ rows: reportRows() }], targetRows: targetRows(), brand: "BrandX", name: "n", idToName });
  const parsed = readMetricSeries(detectMetricsByDate(reportRows()), reportRows());
  assert.equal(matchProfile(parsed, [profile], "SomeOtherBrand"), null);

  const unrelated = makeReport({ days, metrics: [{ label: "Alpha", values: [1, 2, 3, 4, 5, 6, 7] }, { label: "Beta", values: [1, 2, 3, 4, 5, 6, 7] }] });
  const p2 = readMetricSeries(detectMetricsByDate(unrelated), unrelated);
  assert.equal(matchProfile(p2, [profile], "BrandX"), null);
});

test("a report with no profile is transposed: every metric becomes a column", () => {
  const rows = reportRows();
  const parsed = readMetricSeries(detectMetricsByDate(rows), rows);
  const { records, store } = transposeReport(parsed, idToName);
  assert.equal(store.resolved, true);
  assert.equal(records.length, 7);
  assert.equal(records[0].StoreName, "JJS001");
  assert.equal(records[0]["Royalty Sales"], m.royalty[0]);
  assert.equal(records[0]["A/R Due CC (InShop) Amex"], m.amexIn[0]);
  assert.ok("PDQ/Mx Tax Check" in records[0]);
});

test("learning refuses samples that do not overlap", () => {
  assert.throws(
    () => learnProfile({
      sources: [{ rows: reportRows() }],
      targetRows: makeTarget({ storeName: "JJS001", days: week("2030-01-01"), columns: targetColumns() }),
      brand: "B", name: "n", idToName,
    }),
    /at least 3/,
  );
});

// ── the whole pipeline on real files ─────────────────────────────────────────

function writeFiles() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rep-"));
  const write = (name, rows, bookType) => {
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(rows), "Sheet1");
    const file = path.join(dir, name);
    XLSX.writeFile(wb, file, { bookType });
    return file;
  };
  return { dir, write };
}

const baseCtx = (profiles = []) => ({ brand: "BrandX", knownColumns: new Map(), idToName, profiles });

for (const [bookType, ext] of [["biff8", "xls"], ["xlsx", "xlsx"]]) {
  test(`pipeline: a .${ext} report is converted with the learned profile`, async () => {
    const { dir, write } = writeFiles();
    const { profile } = learnProfile({ sources: [{ rows: reportRows() }], targetRows: targetRows(), brand: "BrandX", name: "n", idToName });
    const file = write(`report.${ext}`, makeReport({ store: "9999", days: week("2026-09-02"), metrics: m.list }), bookType);

    const stored = [];
    const summary = await processUploadedFile({ path: file, extension: ext, name: `report.${ext}` }, baseCtx([profile]), async (r) => stored.push(...r));

    assert.equal(stored.length, 7);
    assert.equal(stored[0].StoreName, "JJS002");
    assert.equal(stored[0].Date, "09-02-2026");
    assert.equal(summary.converted[0].mode, "profile");
    assert.equal(summary.converted[0].days, 7);
    assert.equal(summary.converted[0].storeResolved, true);
    fs.rmSync(dir, { recursive: true, force: true });
  });
}

test("pipeline: without a profile the report is transposed and still stored", async () => {
  const { dir, write } = writeFiles();
  const file = write("r.xlsx", reportRows(), "xlsx");
  const stored = [];
  const summary = await processUploadedFile({ path: file, extension: "xlsx", name: "r.xlsx" }, baseCtx([]), async (r) => stored.push(...r));
  assert.equal(stored.length, 7);
  assert.equal(summary.converted[0].mode, "transposed");
  assert.equal(stored[2].Date, mdY(days[2]).replace(/\//g, "-"));
  fs.rmSync(dir, { recursive: true, force: true });
});

test("pipeline: a report with no store anywhere is reported, not guessed", async () => {
  const { dir, write } = writeFiles();
  const rows = reportRows();
  rows[2] = ["Notes:", null, "nothing"]; // remove the store lines
  rows[3] = ["Misc", null, "x"];
  const file = write("r.xlsx", rows, "xlsx");
  const stored = [];
  const summary = await processUploadedFile({ path: file, extension: "xlsx", name: "r.xlsx" }, baseCtx([]), async (r) => stored.push(...r));
  assert.equal(stored.length, 0);
  assert.match(summary.errors[0], /which store/);
  fs.rmSync(dir, { recursive: true, force: true });
});

test("pipeline: a big CSV is streamed in chunks, never all at once", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "csv-"));
  const file = path.join(dir, "big.csv");
  const lines = ["Store Name,Date,Sales,Tips"];
  for (let i = 0; i < 25000; i++) {
    const d = new Date(2026, 0, 1 + (i % 100));
    lines.push(`Store ${i % 50},${mdY(d)},${(i * 1.5).toFixed(2)},${i % 7}`);
  }
  fs.writeFileSync(file, lines.join("\n"));

  const sizes = [];
  const summary = await processUploadedFile(
    { path: file, extension: "csv", name: "big.csv" },
    baseCtx(),
    async (records) => sizes.push(records.length),
    { chunkSize: 4000 },
  );
  assert.equal(sizes.reduce((a, b) => a + b, 0), 25000);
  assert.ok(sizes.length >= 6 && Math.max(...sizes) < 5000, `chunk sizes ${sizes.join(",")}`);
  assert.equal(summary.skipped.missingOrInvalidDate, 0);
  fs.rmSync(dir, { recursive: true, force: true });
});

// ── cell cleaning ────────────────────────────────────────────────────────────

test("numbers written the way reports print them are read as numbers", () => {
  assert.equal(cleanCell("2,979.95"), 2979.95);
  assert.equal(cleanCell("$1,234.50"), 1234.5);
  assert.equal(cleanCell("(12.50)"), -12.5);
  assert.equal(cleanCell("-7"), -7);
  assert.equal(cleanCell(" 42 "), 42);
  assert.equal(cleanCell("PLK 10337"), "PLK 10337");
  assert.equal(cleanCell("1,23"), "1,23"); // not a valid grouping: left alone
  assert.equal(cleanCell(""), "");
  assert.equal(cleanCell(null), null);
  assert.equal(cleanCell(NaN), null);
});

test("edits keep the stored column name exactly", () => {
  const edited = cleanObjectForSampleData({ "Net sales": "1,000.5", Date: "9/1/2026" }, { keepKeys: true });
  assert.deepEqual(edited, { "Net sales": 1000.5, Date: "09-01-2026" });
  // the old behaviour (used for new uploads' legacy columns) strips the space
  assert.deepEqual(Object.keys(cleanObjectForSampleData({ "Net sales": 1 })), ["Netsales"]);
});

test("a flat table still works exactly as before when no report is involved", () => {
  const { records } = extractRecords([{ sheetName: "s", rows: [["Store Name", "Date", "Net sales"], ["PLK 1", "9/1/2026", "1,200.00"]] }]);
  assert.deepEqual(records[0], { StoreName: "PLK 1", Date: "09-01-2026", "Net sales": 1200 });
});
