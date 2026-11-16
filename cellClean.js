import moment from "moment";

// Cell-level cleaning shared by the upload pipeline and the data-update routes.
// Everything here is pure and allocation-light: it runs once per cell, so a
// 100k-row x 40-column file goes through it 4 million times.

export const DATE_FORMATS = [
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
  "DD-MMM-YY",
  "DD-MMM-YYYY",
  "MMM DD, YYYY",
  "MMM-DD-YYYY",
  "YYYYMMDD",
  "M.D.YYYY",
  "MM.DD.YYYY",
  "YYYY.MM.DD",
  "D MMM YYYY",
  "DD MMM YYYY",
];

export const cleanKeyName = (key) => key.trim().replace(/\s+/g, "").replace(/\./g, "_");

// Exact header text, made safe as a MongoDB field name.
export const exactKeyName = (header) =>
  String(header ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .replace(/\./g, "_")
    .replace(/^\$+/, "");

export const normKey = (s) =>
  String(s ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");

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

const NUM_RE = /^-?\d*\.?\d*$/;
// "1,234.50"  "$1,234.50"  "(12.50)"  "-$5"  - exports often format numbers as text
const LOOSE_NUM_RE = /^(\()?(-)?\$?\s?(-)?(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?(\))?$/;

// Parses a number written the way spreadsheets/reports print it. Returns null
// when the text is not a number.
export function looseNumber(text) {
  const m = LOOSE_NUM_RE.exec(text);
  if (!m) return null;
  if (Boolean(m[1]) !== Boolean(m[6])) return null; // unbalanced parentheses
  const n = parseFloat(m[4].replace(/,/g, "") + (m[5] || ""));
  if (Number.isNaN(n)) return null;
  const negative = Boolean(m[1] || m[2] || m[3]);
  return Number((negative ? -n : n).toFixed(2));
}

function convertDateValue(value) {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : moment(value).format("MM-DD-YYYY");
  }
  if (typeof value === "string") {
    value = value.trim().replace(/\u0000/g, "");
    if (value === "") return null;
    // "46083" (a CSV export of an Excel date) is a serial number, not text
    if (/^\d{5}(\.\d+)?$/.test(value)) value = Number(value);
  }
  if (typeof value === "number") {
    if (Number.isNaN(value)) return null;
    if (Number.isInteger(value) && value >= 19000101 && value <= 20991231) {
      // 20260901 style
      const ymd = moment(String(value), "YYYYMMDD", true);
      return ymd.isValid() ? ymd.format("MM-DD-YYYY") : null;
    }
    const excelEpoch = Date.UTC(1899, 11, 31);
    return moment.utc(new Date(excelEpoch + (value - 1) * 86400000)).format("MM-DD-YYYY");
  }
  const parsed = moment(String(value), DATE_FORMATS, true);
  return parsed.isValid() ? parsed.format("MM-DD-YYYY") : null;
}

// Dates repeat constantly (every store has the same days), and parsing one with
// moment against ~30 formats is by far the slowest step - so remember results.
export function cleanDateCell(value, memo = null) {
  if (value === null || value === undefined || value === "") return null;
  if (memo) {
    const hit = memo.get(value);
    if (hit !== undefined) return hit;
    const out = convertDateValue(value);
    memo.set(value, out);
    return out;
  }
  return convertDateValue(value);
}

export function cleanCell(value, isDate = false, memo = null) {
  if (isDate) return cleanDateCell(value, memo);

  if (typeof value === "string") {
    if (value.indexOf("\u0000") !== -1) value = value.replace(/\u0000/g, "");
    value = value.trim();
    if (value === "") return value;
    if (NUM_RE.test(value)) {
      const n = parseFloat(value);
      return Number.isNaN(n) ? value : Number(n.toFixed(2));
    }
    const loose = looseNumber(value);
    return loose === null ? value : loose;
  }
  if (typeof value === "number") {
    return Number.isNaN(value) ? null : Number(value.toFixed(2));
  }
  if (value === null || value === undefined) return null;
  return value;
}

// Used by the data-update routes. keepKeys leaves the field names exactly as
// they are stored (a column called "Net sales" must be updated as "Net sales",
// not silently written to a new "Netsales" field).
export function cleanObjectForSampleData(obj, { keepKeys = false } = {}) {
  const memo = new Map();
  const out = {};
  for (const key of Object.keys(obj)) {
    const name = keepKeys ? key.trim() : cleanKeyName(key);
    out[name] = cleanCell(obj[key], name.toLowerCase() === "date", memo);
  }
  return out;
}

// ─── "does this look like a date?" (used to find the date column by content) ──

const SERIAL_MIN = 42005; // Excel serial for 2015-01-01
const SERIAL_MAX = 49674; // ... 2035-12-31
const dateLikeCache = new Map();

export const isEmptyCell = (v) => v === null || v === undefined || String(v).trim() === "";

export function isDateLike(value, headerHint = false) {
  if (isEmptyCell(value)) return false;
  if (value instanceof Date) return !Number.isNaN(value.getTime());
  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      if (value >= SERIAL_MIN && value <= SERIAL_MAX) return true;
      return (
        value >= 20150101 &&
        value <= 20351231 &&
        moment(String(value), "YYYYMMDD", true).isValid()
      );
    }
    // a decimal (date with a time part) is only believed with a date-ish header
    return headerHint && value >= SERIAL_MIN && value < SERIAL_MAX + 1;
  }
  const text = String(value).trim();
  if (/^\d{5}(\.\d+)?$/.test(text)) {
    const n = Number(text);
    return headerHint && n >= SERIAL_MIN && n <= SERIAL_MAX;
  }
  if (text.length > 30 || !/\d/.test(text)) return false;
  let hit = dateLikeCache.get(text);
  if (hit === undefined) {
    hit = moment(text, DATE_FORMATS, true).isValid();
    if (dateLikeCache.size > 20000) dateLikeCache.clear();
    dateLikeCache.set(text, hit);
  }
  return hit;
}
