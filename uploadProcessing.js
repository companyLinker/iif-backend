import fs from "fs";
import csv from "csv-parser";
import XLSX from "xlsx";
import moment from "moment";

// ─── Row cleaning (shared with the data-update routes) ─────────────────────────

const DATE_FORMATS = [
  "MM-DD-YYYY",
  "MM/DD/YYYY",
  "YYYY-MM-DD",
  "DD-MM-YYYY",
  "DD/MM/YYYY",
  "M/D/YYYY",
  "MM-DD-YY",
  "M-D-YYYY",
  "M/D/YY",
  "MM/DD/YY",
  "M-D-YY",
  "YYYY/MM/DD",
  "YYYY-MM-DD HH:mm:ss",
  "M/D/YYYY H:mm",
  "M/D/YYYY h:mm A",
  "MMM D, YYYY",
  "MMMM D, YYYY",
  "D-MMM-YY",
  "D-MMM-YYYY",
];

export function cleanObjectForSampleData(obj) {
  return Object.keys(obj).reduce((acc, key) => {
    const cleanKey = key.trim().replace(/\s+/g, "").replace(/\./g, "_");
    let value = obj[key];

    if (typeof value === "string") {
      value = value.trim().replace(/\x00/g, "");
      if (cleanKey.toLowerCase() !== "date") {
        const parsedNumber = parseFloat(value.replace(/[^0-9.-]/g, ""));
        if (!isNaN(parsedNumber) && value.match(/^-?\d*\.?\d*$/)) {
          value = Number(parsedNumber.toFixed(2));
        }
      }
    } else if (typeof value === "number" && !isNaN(value)) {
      value = Number(value.toFixed(2));
    } else if (value === null || value === undefined) {
      value = null;
    }

    if (cleanKey.toLowerCase() === "date") {
      if (typeof value === "number") {
        const excelEpoch = new Date(Date.UTC(1899, 11, 31));
        const utcDate = new Date(excelEpoch.getTime() + (value - 1) * 86400000);
        value = moment.utc(utcDate).format("MM-DD-YYYY");
      } else if (value) {
        const parsedDate = moment(String(value), DATE_FORMATS, true);
        if (parsedDate.isValid()) {
          value = parsedDate.format("MM-DD-YYYY");
        } else {
          value = null;
        }
      }
    }

    acc[cleanKey] = value;
    return acc;
  }, {});
}

// ─── Header recognition ────────────────────────────────────────────────────────

const cleanKeyName = (key) => key.trim().replace(/\s+/g, "").replace(/\./g, "_");

export const normKey = (s) =>
  String(s ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");

// Priority order: an exact "storename"/"date" header always wins, aliases are
// only used when the exact name is not present.
const STORE_ALIASES = [
  "storename",
  "store",
  "storelocation",
  "locationname",
  "location",
  "restaurantname",
  "restaurant",
  "unitname",
  "sitename",
];
const DATE_ALIASES = [
  "date",
  "businessdate",
  "reportdate",
  "salesdate",
  "transactiondate",
];

const HEADER_SCAN_ROWS = 30;

function findAliasIndex(normalizedHeaders, aliases) {
  for (const alias of aliases) {
    const idx = normalizedHeaders.indexOf(alias);
    if (idx !== -1) return idx;
  }
  return -1;
}

// Reports often have title rows above the real header, so look for the first
// row that contains both a store column and a date column.
export function detectHeaderRow(rows) {
  const limit = Math.min(rows.length, HEADER_SCAN_ROWS);
  for (let r = 0; r < limit; r++) {
    const normalized = (rows[r] || []).map(normKey);
    if (
      findAliasIndex(normalized, STORE_ALIASES) !== -1 &&
      findAliasIndex(normalized, DATE_ALIASES) !== -1
    ) {
      return r;
    }
  }
  return -1;
}

// Maps every header cell to the column name it should be stored under:
//  - the store / date columns become exactly "StoreName" / "Date"
//  - headers that match a column the collection already has (ignoring case,
//    spaces and punctuation) reuse that column's name, so the same data never
//    ends up split across "NetSales", "Net Sales" and "NETSALES"
//  - anything else keeps its own name (new columns are never dropped)
export function buildColumnTargets(headerCells, knownColumns = new Map()) {
  const headers = headerCells.map((h) =>
    h === null || h === undefined ? "" : String(h).trim(),
  );
  const normalized = headers.map(normKey);
  const storeIdx = findAliasIndex(normalized, STORE_ALIASES);
  const dateIdx = findAliasIndex(normalized, DATE_ALIASES);

  const used = new Set();
  return headers.map((header, i) => {
    let target;
    if (i === storeIdx) target = "StoreName";
    else if (i === dateIdx) target = "Date";
    else if (!header) target = i === 0 ? "__EMPTY" : `__EMPTY_${i}`;
    else {
      const known = knownColumns.get(normalized[i]);
      // New columns follow the existing naming rule (no spaces, "." -> "_").
      target = known && known !== "StoreName" && known !== "Date" ? known : cleanKeyName(header);
    }

    let unique = target;
    for (let n = 2; used.has(unique); n++) unique = `${target}_${n}`;
    used.add(unique);
    return unique;
  });
}

// ─── File reading ──────────────────────────────────────────────────────────────

const isBlankRow = (row) =>
  !row || row.every((c) => c === null || c === undefined || c === "");

export async function readUploadedFile(filePath, extension) {
  if (extension === "csv") {
    const rows = await new Promise((resolve, reject) => {
      const out = [];
      fs.createReadStream(filePath)
        .pipe(csv({ headers: false }))
        .on("data", (row) => {
          const keys = Object.keys(row).sort((a, b) => Number(a) - Number(b));
          out.push(keys.map((k) => (row[k] === "" ? null : row[k])));
        })
        .on("end", () => resolve(out))
        .on("error", reject);
    });
    return [{ sheetName: "csv", rows }];
  }

  if (extension === "xlsx" || extension === "xls") {
    const workbook = XLSX.readFile(filePath);
    return workbook.SheetNames.map((sheetName) => ({
      sheetName,
      rows: XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], {
        header: 1,
        defval: null,
      }),
    }));
  }

  throw new Error("Invalid file format");
}

// Turns the raw rows of every sheet into cleaned records and reports exactly
// why any row was left out (instead of silently dropping it).
export function extractRecords(sheets, knownColumns = new Map()) {
  const records = [];
  const skipped = { missingStoreName: 0, missingOrInvalidDate: 0, samples: [] };
  const detectedColumns = [];
  const sheetsWithoutHeader = [];

  for (const { sheetName, rows } of sheets) {
    if (!rows || rows.length === 0) continue;

    let headerRow = detectHeaderRow(rows);
    if (headerRow === -1) {
      sheetsWithoutHeader.push(sheetName);
      headerRow = rows.findIndex((r) => !isBlankRow(r));
      if (headerRow === -1) continue;
    }

    const targets = buildColumnTargets(rows[headerRow], knownColumns);
    if (detectedColumns.length === 0) detectedColumns.push(...targets);

    // cleanObjectForSampleData strips spaces from keys, which would split
    // "Transaction Count" (already stored) from "TransactionCount". Clean the
    // values under space-free keys, then restore the exact target names.
    const cleanKeys = [];
    const restore = new Map();
    for (const target of targets) {
      let key = cleanKeyName(target);
      for (let n = 2; restore.has(key); n++) key = `${cleanKeyName(target)}_${n}`;
      restore.set(key, target);
      cleanKeys.push(key);
    }

    for (let r = headerRow + 1; r < rows.length; r++) {
      const row = rows[r];
      if (isBlankRow(row)) continue;

      const raw = {};
      for (let c = 0; c < row.length; c++) {
        const key = cleanKeys[c] ?? `__EMPTY_${c}`;
        raw[key] = row[c] === undefined ? null : row[c];
      }
      const cleaned = cleanObjectForSampleData(raw);
      const record = {};
      for (const [key, value] of Object.entries(cleaned)) {
        record[restore.get(key) ?? key] = value;
      }

      const hasStore =
        record.StoreName !== null &&
        record.StoreName !== undefined &&
        String(record.StoreName).trim() !== "";
      const hasDate = Boolean(record.Date);

      if (hasStore && hasDate) {
        records.push(record);
        continue;
      }
      if (!hasStore) skipped.missingStoreName++;
      else skipped.missingOrInvalidDate++;
      if (skipped.samples.length < 5) {
        skipped.samples.push({
          sheet: sheetName,
          row: r + 1,
          reason: !hasStore ? "missing StoreName" : "missing or invalid Date",
        });
      }
    }
  }

  return { records, skipped, detectedColumns, sheetsWithoutHeader };
}

// ─── Storing without duplicates or data loss ───────────────────────────────────

export const recordKey = (record) =>
  `${String(record.StoreName).toLowerCase().replace(/\s+/g, " ").trim()}|${record.Date}`;

const isEmptyValue = (v) => v === null || v === undefined || v === "";

// Fields the record has that the stored document lacks (never overwrites a
// value that is already stored).
function missingFields(existing, incoming) {
  const out = {};
  for (const [field, value] of Object.entries(incoming)) {
    if (field === "_id" || isEmptyValue(value)) continue;
    if (isEmptyValue(existing[field])) out[field] = value;
  }
  return out;
}

export async function loadKnownColumns(collection, sampleSize = 200) {
  const docs = await collection.find({}).limit(sampleSize).toArray();
  const known = new Map();
  for (const doc of docs) {
    for (const key of Object.keys(doc)) {
      if (key === "_id") continue;
      const n = normKey(key);
      if (!known.has(n)) known.set(n, key);
    }
  }
  return known;
}

export async function storeRecords(collection, records, { dateChunk = 100 } = {}) {
  const result = { inserted: 0, duplicates: 0, updated: 0, errors: [] };

  // 1. Collapse duplicates inside the upload itself; a later copy can still
  //    contribute fields the first copy lacked.
  const unique = new Map();
  for (const record of records) {
    const key = recordKey(record);
    const first = unique.get(key);
    if (!first) {
      unique.set(key, record);
      continue;
    }
    result.duplicates++;
    Object.assign(first, missingFields(first, record));
  }

  // 2. Compare against what is already stored, a few dates at a time.
  const byDate = new Map();
  for (const record of unique.values()) {
    if (!byDate.has(record.Date)) byDate.set(record.Date, []);
    byDate.get(record.Date).push(record);
  }
  const dates = [...byDate.keys()];

  for (let i = 0; i < dates.length; i += dateChunk) {
    const chunkDates = dates.slice(i, i + dateChunk);
    try {
      const existingDocs = await collection
        .find({ Date: { $in: chunkDates } })
        .toArray();
      const existingByKey = new Map();
      for (const doc of existingDocs) {
        if (doc.StoreName === undefined || doc.StoreName === null) continue;
        const key = recordKey(doc);
        if (!existingByKey.has(key)) existingByKey.set(key, doc);
      }

      const toInsert = [];
      const updates = [];
      for (const date of chunkDates) {
        for (const record of byDate.get(date)) {
          const existing = existingByKey.get(recordKey(record));
          if (!existing) {
            toInsert.push(record);
            continue;
          }
          result.duplicates++;
          const fields = missingFields(existing, record);
          if (Object.keys(fields).length > 0) {
            updates.push({
              updateOne: { filter: { _id: existing._id }, update: { $set: fields } },
            });
          }
        }
      }

      if (toInsert.length > 0) {
        try {
          const res = await collection.insertMany(toInsert, { ordered: false });
          result.inserted += res.insertedCount;
        } catch (err) {
          // ordered:false keeps going after a failed document; count what landed.
          const landed = err.insertedCount ?? err.result?.insertedCount ?? 0;
          result.inserted += landed;
          result.errors.push(`Some rows could not be inserted: ${err.message}`);
        }
      }
      if (updates.length > 0) {
        const res = await collection.bulkWrite(updates, { ordered: false });
        result.updated += res.modifiedCount ?? updates.length;
      }
    } catch (err) {
      result.errors.push(err.message);
    }
  }

  return result;
}

// ─── One upload at a time per brand ────────────────────────────────────────────
// Two simultaneous uploads of the same data would otherwise both see "not
// stored yet" and both insert it.

const brandLocks = new Map();

export async function withBrandLock(brand, fn) {
  const previous = brandLocks.get(brand) || Promise.resolve();
  let release;
  const mine = new Promise((resolve) => (release = resolve));
  const chain = previous.then(() => mine);
  brandLocks.set(brand, chain);
  await previous;
  try {
    return await fn();
  } finally {
    release();
    if (brandLocks.get(brand) === chain) brandLocks.delete(brand);
  }
}
