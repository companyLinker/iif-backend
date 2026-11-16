import fs from "fs";
import csv from "csv-parser";
import XLSX from "xlsx";
import {
  cleanCell,
  cleanObjectForSampleData,
  exactKeyName,
  isDateLike,
  isEmptyCell,
  normId,
  normKey,
} from "./cellClean.js";

export { cleanObjectForSampleData, normId, normKey };

// ─── Header recognition ────────────────────────────────────────────────────────

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
export const STORE_ID_SET = new Set(STORE_ID_ALIASES);

const HEADER_SCAN_ROWS = 30;
const LOOKAHEAD_ROWS = 250; // rows read before the layout of a sheet is decided
const DATE_HINT = /date|day|period|dt|time|business|when/;
const METRIC_WORDS = /sales|tax|amount|total|count|tip|fee|price|cost|discount|refund|qty|quantity/;

export const isBlankRow = (row) => {
  if (!row) return true;
  for (let i = 0; i < row.length; i++) {
    const v = row[i];
    if (v !== null && v !== undefined && v !== "") return false;
  }
  return true;
};

function findAliasIndex(normalizedHeaders, aliases) {
  for (const alias of aliases) {
    const idx = normalizedHeaders.indexOf(alias);
    if (idx !== -1) return idx;
  }
  return -1;
}

// Reports often have title rows above the real header, so look for the first
// row that has a date column and some way of identifying the store.
export function detectHeaderRow(rows) {
  const limit = Math.min(rows.length, HEADER_SCAN_ROWS);
  for (let r = 0; r < limit; r++) {
    const normalized = Array.from(rows[r] || [], normKey);
    const hasStore =
      findAliasIndex(normalized, STORE_ALIASES) !== -1 ||
      findAliasIndex(normalized, STORE_ID_ALIASES) !== -1;
    if (hasStore && findAliasIndex(normalized, DATE_ALIASES) !== -1) return r;
  }
  return -1;
}

const columnSample = (dataRows, c, limit = 200) => {
  const out = [];
  for (const row of dataRows) {
    const v = row?.[c];
    if (!isEmptyCell(v)) out.push(v);
    if (out.length >= limit) break;
  }
  return out;
};

// The date column: an obvious "Date" header wins; otherwise the column whose
// values are (nearly) all dates, preferring one whose header also looks like a date.
function findDateColumn(headerCells, dataRows) {
  const normalized = headerCells.map(normKey);
  const exact = findAliasIndex(normalized, DATE_ALIASES);
  if (exact !== -1) return exact;

  let best = -1;
  let bestScore = 0;
  for (let c = 0; c < headerCells.length; c++) {
    const sample = columnSample(dataRows, c);
    if (sample.length === 0) continue;
    const hint = DATE_HINT.test(normalized[c]);
    const ratio = sample.filter((v) => isDateLike(v, hint)).length / sample.length;
    if (ratio < 0.8) continue;
    const score = ratio + (hint ? 0.5 : 0);
    if (score > bestScore) {
      best = c;
      bestScore = score;
    }
  }
  return best;
}

// Header row when no header says "date": the first mostly-text row that sits
// directly above rows with a column full of dates.
export function detectHeaderRowByContent(rows) {
  const limit = Math.min(rows.length - 1, HEADER_SCAN_ROWS);
  for (let r = 0; r < limit; r++) {
    const cells = rows[r] || [];
    const filled = Array.from(cells).filter((v) => !isEmptyCell(v));
    if (filled.length < 2) continue;
    const textual = filled.filter(
      (v) => typeof v === "string" && isNaN(Number(v)) && !isDateLike(v),
    ).length;
    if (textual / filled.length < 0.8) continue;

    const body = rows.slice(r + 1, r + 9).filter((row) => !isBlankRow(row));
    if (body.length === 0) continue;
    const width = Math.max(cells.length, ...body.map((b) => b.length));
    for (let c = 0; c < width; c++) {
      const values = body.map((b) => b[c]).filter((v) => !isEmptyCell(v));
      if (values.length < Math.ceil(body.length * 0.8)) continue;
      const hint = DATE_HINT.test(normKey(cells[c]));
      const dates = values.filter((v) => isDateLike(v, hint)).length;
      if (dates / values.length >= 0.8) return r;
    }
  }
  return -1;
}

// Decides which header is the store name, which is the store id and which is
// the date. A weakly-named column ("Store", "Location") whose values are mostly
// ids we know is treated as an id column, not a name. When no header names a
// store at all, a header that merely starts with "store"/"location" is used, and
// as a last resort the column whose values are known store ids or names.
// A value counts as a known store id only if it looks like one: 3+ characters.
const isKnownId = (value, idToName) => {
  const id = normId(value);
  return id.length >= 3 && idToName.has(id);
};

export function locateColumns(headerCells, dataRows = [], idToName = new Map()) {
  const normalized = headerCells.map(normKey);
  const dateIdx = findDateColumn(headerCells, dataRows);
  const exactNameIdx = normalized.indexOf("storename");
  let nameIdx = exactNameIdx !== -1 ? exactNameIdx : findAliasIndex(normalized, STORE_ALIASES);
  let idIdx = findAliasIndex(normalized, STORE_ID_ALIASES);

  if (exactNameIdx === -1 && nameIdx !== -1 && idToName.size > 0) {
    const sample = columnSample(dataRows, nameIdx, 50);
    const known = sample.filter((v) => isKnownId(v, idToName)).length;
    if (sample.length > 0 && known / sample.length >= 0.5 && idIdx === -1) {
      idIdx = nameIdx;
      nameIdx = -1;
    }
  }

  if (nameIdx === -1 && idIdx === -1) {
    for (let c = 0; c < normalized.length; c++) {
      const n = normalized[c];
      if (c === dateIdx || !/^(store|location|restaurant|shop|branch)/.test(n)) continue;
      if (METRIC_WORDS.test(n)) continue;
      if (/(number|num|no|id|code)/.test(n.replace(/^(store|location|restaurant|shop|branch)/, ""))) {
        if (idIdx === -1) idIdx = c;
      } else if (nameIdx === -1) {
        nameIdx = c;
      }
    }
  }

  // No header names a store: look at what the columns contain. A column of
  // known store NAMES wins; a column of known store IDS is only believed when
  // nearly every value is one (a numeric measure such as 0-8 must never pass).
  if (nameIdx === -1 && idIdx === -1 && idToName.size > 0) {
    const knownNames = new Set([...idToName.values()].map((v) => String(v).trim().toLowerCase()));
    const best = { name: { c: -1, ratio: 0 }, id: { c: -1, ratio: 0 } };
    for (let c = 0; c < normalized.length; c++) {
      if (c === dateIdx) continue;
      const sample = columnSample(dataRows, c, 100);
      if (sample.length === 0) continue;
      const nameRatio =
        sample.filter((v) => knownNames.has(String(v).trim().toLowerCase())).length / sample.length;
      const idRatio = sample.filter((v) => isKnownId(v, idToName)).length / sample.length;
      if (nameRatio > best.name.ratio) best.name = { c, ratio: nameRatio };
      if (idRatio > best.id.ratio) best.id = { c, ratio: idRatio };
    }
    if (best.name.ratio >= 0.6) nameIdx = best.name.c;
    else if (best.id.ratio >= 0.9) idIdx = best.id.c;
  }

  // Last resort: nothing is named or recognisable as a store, but there is a
  // date. The store is then the text column that is filled on (nearly) every
  // row and has the most different values. The caller reports that this was a
  // guess, so a wrong one is visible instead of silent.
  let guessed = false;
  if (nameIdx === -1 && idIdx === -1 && dateIdx !== -1) {
    const rowsWithDate = columnSample(dataRows, dateIdx, 250).length;
    let best = -1;
    let bestDistinct = 0;
    for (let c = 0; c < normalized.length; c++) {
      if (c === dateIdx || rowsWithDate === 0) continue;
      const sample = columnSample(dataRows, c, 250);
      if (sample.length / rowsWithDate < 0.9) continue;
      const texty = sample.filter(
        (v) => typeof v === "string" && v.length <= 60 && isNaN(Number(v)) && !isDateLike(v),
      ).length;
      if (texty / sample.length < 0.9) continue;
      const distinct = new Set(sample.map((v) => String(v).trim().toLowerCase())).size;
      if (distinct > bestDistinct) {
        best = c;
        bestDistinct = distinct;
      }
    }
    if (best !== -1) {
      nameIdx = best;
      guessed = true;
    }
  }
  return { nameIdx, idIdx, dateIdx, guessed };
}

// Maps every header cell to the column name it should be stored under:
//  - the store name / date columns become exactly "StoreName" / "Date"
//  - headers that match a column the collection already has (ignoring case,
//    spaces and punctuation) reuse that column's name, so the same data never
//    ends up split across "NetSales", "Net Sales" and "NETSALES"
//  - anything else is stored exactly as written in the file (new columns are
//    never dropped or renamed), and a store-id column is kept as a normal
//    column next to the resolved StoreName
export function buildColumnTargets(headerCells, knownColumns = new Map(), roles = null) {
  const headers = Array.from(headerCells, (h) =>
    h === null || h === undefined ? "" : String(h).trim(),
  );
  const normalized = headers.map(normKey);
  const { nameIdx, dateIdx } = roles ?? locateColumns(headers);

  const used = new Set();
  return headers.map((header, i) => {
    let target;
    if (i === nameIdx) target = "StoreName";
    else if (i === dateIdx) target = "Date";
    else if (!header) target = i === 0 ? "__EMPTY" : `__EMPTY_${i}`;
    else {
      const known = knownColumns.get(normalized[i]);
      target = known && known !== "StoreName" && known !== "Date" ? known : exactKeyName(header);
    }

    let unique = target;
    for (let n = 2; used.has(unique); n++) unique = `${target}_${n}`;
    used.add(unique);
    return unique;
  });
}

// ─── File reading ──────────────────────────────────────────────────────────────

// Streams a CSV one row at a time (rows are arrays, empty cells are null) so a
// huge file never has to fit in memory.
export async function* csvRowStream(filePath) {
  const stream = fs.createReadStream(filePath).pipe(csv({ headers: false }));
  try {
    for await (const row of stream) {
      // csv-parser keys are "0","1","2"... - integer keys iterate in order
      const values = Object.values(row);
      for (let i = 0; i < values.length; i++) if (values[i] === "") values[i] = null;
      yield values;
    }
  } finally {
    stream.destroy();
  }
}

const XLSX_READ_OPTIONS = {
  dense: true, // arrays instead of one object per cell: far less memory/time
  cellFormula: false,
  cellHTML: false,
  cellNF: false,
  cellStyles: false,
  sheetStubs: false,
};

export function readWorkbookSheets(filePath) {
  const workbook = XLSX.readFile(filePath, XLSX_READ_OPTIONS);
  return workbook.SheetNames.map((sheetName) => ({
    sheetName,
    rows: XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], {
      header: 1,
      defval: null,
    }),
  }));
}

export async function readUploadedFile(filePath, extension) {
  if (extension === "csv" || extension === "txt") {
    const rows = [];
    for await (const row of csvRowStream(filePath)) rows.push(row);
    return [{ sheetName: "csv", rows }];
  }
  if (extension === "xlsx" || extension === "xls") return readWorkbookSheets(filePath);
  throw new Error("Invalid file format");
}

// ─── Turning rows into records ─────────────────────────────────────────────────

export const newSkipped = () => ({ missingStoreName: 0, missingOrInvalidDate: 0, samples: [] });
export const newStoreIds = () => ({ column: null, guessedColumn: null, resolved: 0, unresolved: new Set() });

// Feeds on the rows of ONE sheet and produces cleaned records. The layout
// (header row, date/store columns) is decided from the first rows, then every
// further row is converted with the per-column information prepared once - so
// the cost per row is a single pass over its cells. Records can be drained in
// chunks while rows are still arriving, which keeps memory flat for huge files.
export class SheetExtractor {
  constructor(
    sheetName,
    {
      knownColumns = new Map(),
      idToName = new Map(),
      skipped = newSkipped(),
      storeIds = newStoreIds(),
      detectedColumns = [],
    } = {},
  ) {
    this.sheetName = sheetName;
    this.known = knownColumns;
    this.idToName = idToName;
    this.skipped = skipped;
    this.storeIds = storeIds;
    this.detectedColumns = detectedColumns;
    this.noHeader = false;
    this.out = [];
    this._buffer = [];
    this._locked = false;
    this._empty = false;
    this._rowNo = 0;
  }

  push(row) {
    this._rowNo++;
    if (!this._locked) {
      this._buffer.push(row);
      if (this._buffer.length >= LOOKAHEAD_ROWS) this._lock();
      return;
    }
    this._process(row, this._rowNo);
  }

  finish() {
    if (!this._locked) this._lock();
  }

  drain() {
    const out = this.out;
    this.out = [];
    return out;
  }

  _lock() {
    this._locked = true;
    const rows = this._buffer;
    this._buffer = [];

    let headerRow = detectHeaderRow(rows);
    if (headerRow === -1) headerRow = detectHeaderRowByContent(rows);
    if (headerRow === -1) {
      this.noHeader = true;
      headerRow = rows.findIndex((r) => !isBlankRow(r));
      if (headerRow === -1) {
        this._empty = true;
        return;
      }
    }

    const headerCells = Array.from(rows[headerRow]);
    this.roles = locateColumns(headerCells, rows.slice(headerRow + 1), this.idToName);
    this.targets = buildColumnTargets(headerCells, this.known, this.roles);
    if (this.detectedColumns.length === 0) this.detectedColumns.push(...this.targets);
    this.isDate = this.targets.map((_, c) => c === this.roles.dateIdx);
    if (this.roles.guessed) {
      this.guessedStoreColumn = String(headerCells[this.roles.nameIdx] ?? "").trim();
      if (!this.storeIds.guessedColumn) this.storeIds.guessedColumn = this.guessedStoreColumn;
    }
    this.dateMemo = new Map();

    // No store name column: the name is looked up from the store id column.
    this.idTarget =
      this.roles.nameIdx === -1 && this.roles.idIdx !== -1 ? this.targets[this.roles.idIdx] : null;
    if (this.idTarget && !this.storeIds.column) this.storeIds.column = this.idTarget;

    for (let i = headerRow + 1; i < rows.length; i++) this._process(rows[i], i + 1);
  }

  _process(row, rowNo) {
    if (this._empty || isBlankRow(row)) return;
    const targets = this.targets;
    const isDate = this.isDate;
    const memo = this.dateMemo;

    let record = {};
    for (let c = 0; c < row.length; c++) {
      const v = row[c];
      record[targets[c] ?? `__EMPTY_${c}`] = cleanCell(
        v === undefined ? null : v,
        isDate[c] === true,
        memo,
      );
    }

    if (this.idTarget) {
      const idValue = record[this.idTarget];
      if (!isEmptyCell(idValue)) {
        const name = this.idToName.get(normId(idValue));
        if (name) this.storeIds.resolved++;
        else this.storeIds.unresolved.add(String(idValue).trim());
        record = { StoreName: name ?? String(idValue).trim(), ...record };
      }
    }

    const hasStore = !isEmptyCell(record.StoreName);
    const hasDate = Boolean(record.Date);
    if (hasStore && hasDate) {
      this.out.push(record);
      return;
    }

    if (!hasStore) this.skipped.missingStoreName++;
    else this.skipped.missingOrInvalidDate++;
    if (this.skipped.samples.length < 5) {
      this.skipped.samples.push({
        sheet: this.sheetName,
        row: rowNo,
        reason: !hasStore ? "missing StoreName / store id" : "missing or invalid Date",
      });
    }
  }
}

// Turns the raw rows of every sheet into cleaned records and reports exactly
// why any row was left out (instead of silently dropping it).
export function extractRecords(sheets, knownColumns = new Map(), idToName = new Map()) {
  const records = [];
  const skipped = newSkipped();
  const detectedColumns = [];
  const sheetsWithoutHeader = [];
  const storeIds = newStoreIds();

  for (const { sheetName, rows } of sheets) {
    if (!rows || rows.length === 0) continue;
    const extractor = new SheetExtractor(sheetName, {
      knownColumns,
      idToName,
      skipped,
      storeIds,
      detectedColumns,
    });
    for (let i = 0; i < rows.length; i++) extractor.push(rows[i]);
    extractor.finish();
    if (extractor.noHeader) sheetsWithoutHeader.push(sheetName);
    const out = extractor.drain();
    for (let i = 0; i < out.length; i++) records.push(out[i]);
  }

  return { records, skipped, detectedColumns, sheetsWithoutHeader, storeIds };
}

// ─── Storing without duplicates or data loss ───────────────────────────────────

export const recordKey = (record) =>
  `${String(record.StoreName).toLowerCase().replace(/\s+/g, " ").trim()}|${record.Date}`;

const isEmptyValue = (v) => v === null || v === undefined || v === "";

const idFieldCache = new Map();
const isIdField = (key) => {
  let hit = idFieldCache.get(key);
  if (hit === undefined) {
    hit = STORE_ID_SET.has(normKey(key));
    idFieldCache.set(key, hit);
  }
  return hit;
};

// The store id a record carries (if it has a StoreNumber / StoreNo / ... column).
export function storeIdOf(record) {
  for (const key in record) {
    const value = record[key];
    if (!isEmptyValue(value) && isIdField(key)) return normId(value);
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
  for (const field in incoming) {
    const value = incoming[field];
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
    for (const group of String(doc.StoreName).match(/\d{4,}/g) || []) add(group, doc.StoreName);
  }

  if (storeDataCollection) {
    const stores = await storeDataCollection.find({}).toArray();
    for (const store of stores) {
      if (!isEmptyValue(store.NO) && !isEmptyValue(store.NAME)) add(store.NO, store.NAME);
    }
  }
  return map;
}

async function mapLimit(items, limit, fn) {
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const item = items[next++];
      await fn(item);
    }
  });
  await Promise.all(workers);
}

// Stores one batch of records. Call it once per chunk of a large upload: each
// call only loads the already-stored rows that could clash with this batch.
export async function storeRecords(collection, records, { dateChunk = 100, concurrency = 4 } = {}) {
  const result = { inserted: 0, duplicates: 0, updated: 0, errors: [] };

  // 1. Collapse duplicates inside the batch; a later copy can still
  //    contribute fields the first copy lacked.
  const seen = new Map();
  const unique = [];
  for (const record of records) {
    const keys = identityKeys(record);
    let first;
    for (const k of keys) {
      first = seen.get(k);
      if (first) break;
    }
    if (!first) {
      for (const k of keys) seen.set(k, record);
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
  const dateGroups = [];
  for (let i = 0; i < dates.length; i += dateChunk) dateGroups.push(dates.slice(i, i + dateChunk));

  await mapLimit(dateGroups, concurrency, async (chunkDates) => {
    try {
      const batch = chunkDates.flatMap((d) => byDate.get(d));

      // Only fetch stored rows that can clash with this batch (same date AND
      // same store name or store id) instead of every row of those dates.
      const names = new Set();
      const idsByField = new Map();
      for (const record of batch) {
        names.add(record.StoreName);
        for (const key in record) {
          if (!isIdField(key) || isEmptyValue(record[key])) continue;
          if (!idsByField.has(key)) idsByField.set(key, new Set());
          idsByField.get(key).add(record[key]);
        }
      }
      const or = [{ StoreName: { $in: [...names] } }];
      for (const [field, values] of idsByField) or.push({ [field]: { $in: [...values] } });

      const existingDocs = await collection
        .find({ Date: { $in: chunkDates }, $or: or })
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
      for (const record of batch) {
        let existing;
        for (const k of identityKeys(record)) {
          existing = existingByKey.get(k);
          if (existing) break;
        }
        if (!existing) {
          toInsert.push(record);
          continue;
        }
        result.duplicates++;
        const fields = missingFields(existing, record);
        // A row stored earlier under its raw id (name was unknown then) gets
        // the proper store name once it can be resolved.
        const id = storeIdOf(record);
        if (id && record.StoreName !== existing.StoreName && normId(existing.StoreName) === id) {
          fields.StoreName = record.StoreName;
        }
        for (const _ in fields) {
          updates.push({
            updateOne: { filter: { _id: existing._id }, update: { $set: fields } },
          });
          break;
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
  });

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
