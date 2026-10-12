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

// Columns that hold a store *identifier* (number or code) rather than a name.
const STORE_ID_ALIASES = [
  "storenumber",
  "storeno",
  "storenum",
  "storeid",
  "storecode",
  "locationnumber",
  "locationno",
  "locationid",
  "unitnumber",
  "unitno",
  "unitid",
  "restaurantnumber",
  "restaurantno",
  "restaurantid",
  "sitenumber",
  "siteid",
];
const STORE_ID_SET = new Set(STORE_ID_ALIASES);

const HEADER_SCAN_ROWS = 30;

function findAliasIndex(normalizedHeaders, aliases) {
  for (const alias of aliases) {
    const idx = normalizedHeaders.indexOf(alias);
    if (idx !== -1) return idx;
  }
  return -1;
}

// "02384", "#2384", " 2384.0 " and 2384 are the same store id; text ids such as
// "Burger Exp 1" are compared ignoring case and spacing.
export function normId(value) {
  let s = String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/^#\s*/, "")
    .replace(/\s+/g, " ");
  if (/^\d+(\.0+)?$/.test(s)) s = s.replace(/\.0+$/, "").replace(/^0+(?=\d)/, "");
  return s;
}

// Reports often have title rows above the real header, so look for the first
// row that has a date column and some way of identifying the store.
export function detectHeaderRow(rows) {
  const limit = Math.min(rows.length, HEADER_SCAN_ROWS);
  for (let r = 0; r < limit; r++) {
    const normalized = (rows[r] || []).map(normKey);
    const hasStore =
      findAliasIndex(normalized, STORE_ALIASES) !== -1 ||
      findAliasIndex(normalized, STORE_ID_ALIASES) !== -1;
    if (hasStore && findAliasIndex(normalized, DATE_ALIASES) !== -1) return r;
  }
  return -1;
}

// Decides which header is the store name, which is the store id and which is
// the date. A weakly-named column ("Store", "Location") whose values are mostly
// ids we know is treated as an id column, not a name.
export function locateColumns(headerCells, dataRows = [], idToName = new Map()) {
  const normalized = headerCells.map(normKey);
  const dateIdx = findAliasIndex(normalized, DATE_ALIASES);
  const exactNameIdx = normalized.indexOf("storename");
  let nameIdx = exactNameIdx !== -1 ? exactNameIdx : findAliasIndex(normalized, STORE_ALIASES);
  let idIdx = findAliasIndex(normalized, STORE_ID_ALIASES);

  if (exactNameIdx === -1 && nameIdx !== -1 && idToName.size > 0) {
    const sample = [];
    for (const row of dataRows) {
      const v = row?.[nameIdx];
      if (v !== null && v !== undefined && String(v).trim() !== "") sample.push(v);
      if (sample.length >= 50) break;
    }
    const known = sample.filter((v) => idToName.has(normId(v))).length;
    if (sample.length > 0 && known / sample.length >= 0.5 && idIdx === -1) {
      idIdx = nameIdx;
      nameIdx = -1;
    }
  }
  return { nameIdx, idIdx, dateIdx };
}

// Maps every header cell to the column name it should be stored under:
//  - the store name / date columns become exactly "StoreName" / "Date"
//  - headers that match a column the collection already has (ignoring case,
//    spaces and punctuation) reuse that column's name, so the same data never
//    ends up split across "NetSales", "Net Sales" and "NETSALES"
//  - anything else keeps its own name (new columns are never dropped), and a
//    store-id column is kept as a normal column next to the resolved StoreName
export function buildColumnTargets(headerCells, knownColumns = new Map(), roles = null) {
  const headers = headerCells.map((h) =>
    h === null || h === undefined ? "" : String(h).trim(),
  );
  const normalized = headers.map(normKey);
  const { nameIdx, dateIdx } = roles ?? locateColumns(headerCells);

  const used = new Set();
  return headers.map((header, i) => {
    let target;
    if (i === nameIdx) target = "StoreName";
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
export function extractRecords(sheets, knownColumns = new Map(), idToName = new Map()) {
  const records = [];
  const skipped = { missingStoreName: 0, missingOrInvalidDate: 0, samples: [] };
  const detectedColumns = [];
  const sheetsWithoutHeader = [];
  const storeIds = { column: null, resolved: 0, unresolved: new Set() };

  for (const { sheetName, rows } of sheets) {
    if (!rows || rows.length === 0) continue;

    let headerRow = detectHeaderRow(rows);
    if (headerRow === -1) {
      sheetsWithoutHeader.push(sheetName);
      headerRow = rows.findIndex((r) => !isBlankRow(r));
      if (headerRow === -1) continue;
    }

    const roles = locateColumns(rows[headerRow], rows.slice(headerRow + 1), idToName);
    const targets = buildColumnTargets(rows[headerRow], knownColumns, roles);
    if (detectedColumns.length === 0) detectedColumns.push(...targets);
    // No store name column: the name is looked up from the store id column.
    const idTarget = roles.nameIdx === -1 && roles.idIdx !== -1 ? targets[roles.idIdx] : null;
    if (idTarget && !storeIds.column) storeIds.column = idTarget;

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
      let record = {};
      for (const [key, value] of Object.entries(cleaned)) {
        record[restore.get(key) ?? key] = value;
      }

      if (idTarget && !isEmptyValue(record[idTarget])) {
        const idValue = record[idTarget];
        const name = idToName.get(normId(idValue));
        if (name) storeIds.resolved++;
        else storeIds.unresolved.add(String(idValue).trim());
        record = { StoreName: name ?? String(idValue).trim(), ...record };
      }

      const hasStore = !isEmptyValue(record.StoreName) && String(record.StoreName).trim() !== "";
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
          reason: !hasStore ? "missing StoreName / store id" : "missing or invalid Date",
        });
      }
    }
  }

  return { records, skipped, detectedColumns, sheetsWithoutHeader, storeIds };
}

// ─── Storing without duplicates or data loss ───────────────────────────────────

export const recordKey = (record) =>
  `${String(record.StoreName).toLowerCase().replace(/\s+/g, " ").trim()}|${record.Date}`;

const isEmptyValue = (v) => v === null || v === undefined || v === "";

// The store id a record carries (if it has a StoreNumber / StoreNo / ... column).
export function storeIdOf(record) {
  for (const [key, value] of Object.entries(record)) {
    if (STORE_ID_SET.has(normKey(key)) && !isEmptyValue(value)) return normId(value);
  }
  return null;
}

// Every key two records can be recognised as the same store+day by.
const identityKeys = (record) => {
  const keys = [recordKey(record)];
  const id = storeIdOf(record);
  if (id) keys.push(`#${id}|${record.Date}`);
  return keys;
};

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

// store id -> store name. What this brand's collection already uses wins (so
// names stay consistent), then the StoreData collection (NO -> NAME).
export async function loadStoreIdMap(collection, storeDataCollection, sampleSize = 2000) {
  const map = new Map();
  const add = (id, name) => {
    const key = normId(id);
    if (key && name && !map.has(key)) map.set(key, String(name));
  };

  const docs = await collection.find({}).limit(sampleSize).toArray();
  for (const doc of docs) {
    if (isEmptyValue(doc.StoreName)) continue;
    const id = storeIdOf(doc);
    // Rows stored under their raw id (name unknown at the time) say nothing
    // about the real name, so they must not shadow StoreData.
    if (id && normId(doc.StoreName) === id) continue;
    if (id) add(id, doc.StoreName);
    for (const group of String(doc.StoreName).match(/\d{3,}/g) || []) add(group, doc.StoreName);
  }

  if (storeDataCollection) {
    const stores = await storeDataCollection.find({}).toArray();
    for (const store of stores) {
      if (!isEmptyValue(store.NO) && !isEmptyValue(store.NAME)) add(store.NO, store.NAME);
    }
  }
  return map;
}

export async function storeRecords(collection, records, { dateChunk = 100 } = {}) {
  const result = { inserted: 0, duplicates: 0, updated: 0, errors: [] };

  // 1. Collapse duplicates inside the upload itself; a later copy can still
  //    contribute fields the first copy lacked.
  const seen = new Map();
  const unique = [];
  for (const record of records) {
    const keys = identityKeys(record);
    const first = keys.map((k) => seen.get(k)).find(Boolean);
    if (!first) {
      keys.forEach((k) => seen.set(k, record));
      unique.push(record);
      continue;
    }
    result.duplicates++;
    Object.assign(first, missingFields(first, record));
  }

  // 2. Compare against what is already stored, a few dates at a time.
  const byDate = new Map();
  for (const record of unique) {
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
        if (isEmptyValue(doc.StoreName)) continue;
        for (const key of identityKeys(doc)) {
          if (!existingByKey.has(key)) existingByKey.set(key, doc);
        }
      }

      const toInsert = [];
      const updates = [];
      for (const date of chunkDates) {
        for (const record of byDate.get(date)) {
          const existing = identityKeys(record)
            .map((k) => existingByKey.get(k))
            .find(Boolean);
          if (!existing) {
            toInsert.push(record);
            continue;
          }
          result.duplicates++;
          const fields = missingFields(existing, record);
          // A row stored earlier under its raw id (name was unknown then) gets
          // the proper store name once it can be resolved.
          const id = storeIdOf(record);
          if (
            id &&
            record.StoreName !== existing.StoreName &&
            normId(existing.StoreName) === id
          ) {
            fields.StoreName = record.StoreName;
          }
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
