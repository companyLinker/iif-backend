import { normKey } from "./cellClean.js";
import {
  SheetExtractor,
  csvRowStream,
  newSkipped,
  newStoreIds,
  readWorkbookSheets,
} from "./uploadProcessing.js";
import {
  applyProfile,
  detectMetricsByDate,
  matchProfile,
  readMetricSeries,
  transposeReport,
} from "./reportConverter.js";

export const CHUNK_SIZE = 5000;
const HEAD_ROWS = 300; // enough to decide a sheet's layout before streaming the rest

const iteratorOf = (rows) =>
  rows[Symbol.asyncIterator] ? rows[Symbol.asyncIterator]() : rows[Symbol.iterator]();

// Existing columns of the collection win over the spelling in a new file, so
// "net sales" and "Net Sales" never become two columns.
function reuseKnownColumns(records, known) {
  if (known.size === 0) return records;
  return records.map((record) => {
    const out = {};
    for (const key in record) {
      const name = key === "StoreName" || key === "Date" ? key : (known.get(normKey(key)) ?? key);
      if (!(name in out) || out[name] === null) out[name] = record[key];
    }
    return out;
  });
}

// Reads one uploaded file and hands the records to `storeChunk` in batches, so
// memory stays flat however large the file is. Each sheet is either
//   - a report (metrics down, days across): converted with the matching
//     profile, or transposed as it is if no profile fits; or
//   - a table: read row by row, whatever its column names.
// ctx: { brand, knownColumns, idToName, profiles }
export async function processUploadedFile(file, ctx, storeChunk, { chunkSize = CHUNK_SIZE } = {}) {
  const summary = {
    sheets: 0,
    skipped: newSkipped(),
    storeIds: newStoreIds(),
    detectedColumns: [],
    sheetsWithoutHeader: [],
    converted: [],
    errors: [],
  };

  const sheets =
    file.extension === "csv" || file.extension === "txt"
      ? [{ sheetName: "csv", rows: csvRowStream(file.path) }]
      : file.extension === "xlsx" || file.extension === "xls"
        ? readWorkbookSheets(file.path)
        : (() => {
            throw new Error("Invalid file format");
          })();

  const flush = async (records) => {
    if (records.length > 0) await storeChunk(records);
  };

  const convertReport = async (sheetName, rows) => {
    const layout = detectMetricsByDate(rows);
    const parsed = readMetricSeries(layout, rows);
    const match = matchProfile(parsed, ctx.profiles, ctx.brand);

    const result = match
      ? applyProfile(parsed, match.profile, ctx.idToName)
      : transposeReport(parsed, ctx.idToName);

    const store = result.store;
    const storeName = result.records[0]?.StoreName;
    if (!storeName) {
      summary.errors.push(
        `${file.name} (${sheetName}): could not tell which store this report is for - no store number or name was found in the lines above the table.`,
      );
      return;
    }
    if (store?.resolved) summary.storeIds.resolved += result.records.length;
    else if (store) summary.storeIds.unresolved.add(String(store.id ?? store.value));
    if (!summary.storeIds.column) summary.storeIds.column = "report header";

    const records = reuseKnownColumns(result.records, ctx.knownColumns);
    if (summary.detectedColumns.length === 0 && records[0]) {
      summary.detectedColumns.push(...Object.keys(records[0]));
    }
    summary.converted.push({
      file: file.name,
      sheet: sheetName,
      mode: match ? "profile" : "transposed",
      profile: match?.profile.name ?? null,
      days: records.length,
      from: parsed.dates[0],
      to: parsed.dates[parsed.dates.length - 1],
      store: storeName,
      storeResolved: Boolean(store?.resolved),
      missingLabels: result.missingLabels ?? [],
    });
    await flush(records);
  };

  for (const { sheetName, rows } of sheets) {
    summary.sheets++;
    const it = iteratorOf(rows);

    const head = [];
    let done = false;
    while (head.length < HEAD_ROWS) {
      const next = await it.next();
      if (next.done) {
        done = true;
        break;
      }
      head.push(next.value);
    }
    if (head.length === 0) continue;

    // a report is small: read all of it, then convert
    if (detectMetricsByDate(head)) {
      while (!done) {
        const next = await it.next();
        if (next.done) break;
        head.push(next.value);
      }
      await convertReport(sheetName, head);
      continue;
    }

    // a table: stream it through the extractor, storing every `chunkSize` rows
    const extractor = new SheetExtractor(sheetName, {
      knownColumns: ctx.knownColumns,
      idToName: ctx.idToName,
      skipped: summary.skipped,
      storeIds: summary.storeIds,
      detectedColumns: summary.detectedColumns,
    });
    for (const row of head) extractor.push(row);
    if (extractor.out.length >= chunkSize) await flush(extractor.drain());
    while (!done) {
      const next = await it.next();
      if (next.done) break;
      extractor.push(next.value);
      if (extractor.out.length >= chunkSize) await flush(extractor.drain());
    }
    extractor.finish();
    if (extractor.noHeader) summary.sheetsWithoutHeader.push(sheetName);
    await flush(extractor.drain());
  }

  return summary;
}
