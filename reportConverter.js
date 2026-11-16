import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  cleanCell,
  cleanDateCell,
  isDateLike,
  isEmptyCell,
  normId,
  normKey,
} from "./cellClean.js";
import { extractRecords } from "./uploadProcessing.js";

// Many systems export a *report* instead of a table: one row per metric
// ("Net Sales", "Tax", "DoorDash"...) and one column (or an AM/PM pair of
// columns) per day, with the store written in a few header lines. The upload
// pipeline stores flat rows (one per store and day), so such a report is turned
// into that shape here:
//
//   - a mapping PROFILE (plain data, learned from a sample of the report and a
//     sample of the wanted output) says which report rows make up each output
//     column, so the output has exactly the columns of the sample format;
//   - without a matching profile the report is simply transposed: every metric
//     becomes a column, exactly as written, so nothing is lost.
//
// Nothing in this file knows about a particular brand, store or report.

const HEAD_ROWS = 40;
const toNumber = (v) => {
  const c = cleanCell(v);
  return typeof c === "number" ? c : null;
};
const round2 = (n) => Number(n.toFixed(2));

// ─── Layout detection ──────────────────────────────────────────────────────────

// Finds the row holding the day headers (3+ dates side by side) and works out
// which columns belong to which day, where the metric names are, and what the
// header lines above say (store, week ending...). Returns null for ordinary
// tables.
export function detectMetricsByDate(rows) {
  const limit = Math.min(rows.length, HEAD_ROWS);
  for (let r = 0; r < limit; r++) {
    const row = rows[r];
    if (!row) continue;

    const dateCols = [];
    let filledFromFirstDate = 0;
    let firstDate = -1;
    for (let c = 0; c < row.length; c++) {
      if (isDateLike(row[c])) {
        if (firstDate === -1) firstDate = c;
        dateCols.push(c);
      }
      if (firstDate !== -1 && !isEmptyCell(row[c])) filledFromFirstDate++;
    }
    if (dateCols.length < 3 || dateCols.length / filledFromFirstDate < 0.7) continue;

    // the metric names: the column left of the days that is text on most rows
    let labelCol = -1;
    let bestHits = 0;
    for (let c = 0; c < firstDate; c++) {
      let hits = 0;
      for (let i = r + 1; i < Math.min(rows.length, r + 60); i++) {
        const v = rows[i]?.[c];
        if (typeof v === "string" && v.trim() !== "" && toNumber(v) === null) hits++;
      }
      if (hits > bestHits) {
        bestHits = hits;
        labelCol = c;
      }
    }
    if (labelCol === -1 || bestHits < 3) continue;

    const width = Math.max(row.length, ...rows.slice(r, r + 60).map((x) => x?.length ?? 0));
    const groups = dateCols.map((c, k) => ({
      date: cleanDateCell(row[c]),
      from: c,
      to: (dateCols[k + 1] ?? width) - 1,
    }));
    if (groups.some((g) => !g.date)) continue;

    // header lines above the day row: "Label: | | value" pairs and the title
    const meta = {};
    let title = "";
    for (let i = 0; i < r; i++) {
      const cells = (rows[i] || []).filter((v) => !isEmptyCell(v));
      if (cells.length === 1 && !title) title = String(cells[0]).trim();
      if (cells.length === 2) meta[String(cells[0]).trim().replace(/:$/, "")] = String(cells[1]).trim();
    }

    return { dateRow: r, labelCol, groups, meta, title };
  }
  return null;
}

// Reads every metric row of a detected report: its label and one number per
// day (the AM and PM columns of a day are added together).
export function readMetricSeries(layout, rows) {
  const { dateRow, labelCol, groups } = layout;
  const metrics = [];
  const byKey = new Map();
  const seen = new Map();

  for (let i = dateRow + 1; i < rows.length; i++) {
    const row = rows[i];
    const label = row?.[labelCol];
    if (typeof label !== "string" || label.trim() === "") continue;

    const values = new Array(groups.length).fill(0);
    let numeric = false;
    for (let g = 0; g < groups.length; g++) {
      for (let c = groups[g].from; c <= groups[g].to; c++) {
        const n = toNumber(row[c]);
        if (n !== null) {
          values[g] += n;
          numeric = true;
        }
      }
      values[g] = round2(values[g]);
    }
    if (!numeric) continue; // a text-only row ("Shift Manager - Name")

    let key = normKey(label);
    const n = (seen.get(key) ?? 0) + 1;
    seen.set(key, n);
    if (n > 1) key = `${key}~${n}`;
    const metric = { label: label.trim(), key, values };
    metrics.push(metric);
    byKey.set(key, metric);
  }

  return {
    dates: groups.map((g) => g.date),
    metrics,
    byKey,
    meta: layout.meta,
    title: layout.title,
  };
}

// ─── Store identity ────────────────────────────────────────────────────────────

const STORE_META_KEYS = [
  "storenumber",
  "storeno",
  "storeid",
  "storecode",
  "store",
  "storelocation",
  "location",
  "unitnumber",
  "unit",
  "storename",
];

// A header line such as "4582 - Route 46, Fort Lee, NJ" or "4582" identifies a
// store: try the whole text first, then each number in it.
function resolveStore(value, idToName) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  const candidates = [text, ...(text.match(/\d{3,}/g) || [])];
  for (const candidate of candidates) {
    const name = idToName.get(normId(candidate));
    if (name) return { value: text, id: candidate, name, resolved: true };
  }
  return { value: text, id: candidates[candidates.length > 1 ? 1 : 0], name: null, resolved: false };
}

function findStore(parsed, idToName, metaKey = null) {
  const wanted = metaKey ? [normKey(metaKey)] : STORE_META_KEYS;
  const entries = Object.entries(parsed.meta).map(([k, v]) => [normKey(k), k, v]);
  let fallback = null;
  for (const want of wanted) {
    for (const [norm, , value] of entries) {
      if (norm !== want) continue;
      const found = resolveStore(value, idToName);
      if (found?.resolved) return found;
      fallback = fallback ?? found;
    }
  }
  return fallback;
}

// ─── Profiles ──────────────────────────────────────────────────────────────────

const PROFILE_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "profiles");
let bundledProfiles = null;

// Profiles shipped with the code (backend/profiles/*.json).
export function loadBundledProfiles() {
  if (bundledProfiles) return bundledProfiles;
  bundledProfiles = [];
  try {
    for (const file of fs.readdirSync(PROFILE_DIR)) {
      if (!file.endsWith(".json")) continue;
      try {
        bundledProfiles.push(JSON.parse(fs.readFileSync(path.join(PROFILE_DIR, file), "utf8")));
      } catch (err) {
        console.error(`Ignoring unreadable profile ${file}:`, err.message);
      }
    }
  } catch {
    // no profiles directory: nothing is bundled
  }
  return bundledProfiles;
}

const appliesTo = (profile, brand) =>
  (profile.brands || []).some((b) => b === "*" || normKey(b) === normKey(brand));

// The profile whose known metric names are best covered by this report.
export function matchProfile(parsed, profiles, brand) {
  let best = null;
  for (const profile of profiles) {
    if (profile.layout !== "metrics-by-date" || !appliesTo(profile, brand)) continue;
    const labels = profile.detect?.labels || [];
    if (labels.length === 0) continue;
    const hits = labels.filter((l) => parsed.byKey.has(l)).length;
    const coverage = hits / labels.length;
    if (coverage >= (profile.detect.minCoverage ?? 0.6) && (!best || coverage > best.coverage)) {
      best = { profile, coverage };
    }
  }
  return best;
}

// Builds the flat rows (one per day) a profile describes.
export function applyProfile(parsed, profile, idToName) {
  const store = findStore(parsed, idToName, profile.store?.metaKey);
  const storeName = store?.name ?? store?.value ?? profile.store?.constant ?? null;
  const missing = new Set();

  const metric = (label) => {
    const m = parsed.byKey.get(normKey(label));
    if (!m) missing.add(label);
    return m;
  };

  const records = parsed.dates.map((date, j) => {
    const record = { StoreName: storeName, Date: date };
    for (const col of profile.columns) {
      if (col.blank) {
        record[col.name] = null;
      } else if (col.add?.length || col.subtract?.length) {
        let sum = 0;
        for (const label of col.add || []) sum += metric(label)?.values[j] ?? 0;
        for (const label of col.subtract || []) sum -= metric(label)?.values[j] ?? 0;
        record[col.name] = round2(sum);
      } else {
        record[col.name] = col.constant ?? 0;
      }
    }
    return record;
  });

  return { records, store, missingLabels: [...missing] };
}

// No profile: every metric becomes a column, named as it is written.
export function transposeReport(parsed, idToName) {
  const store = findStore(parsed, idToName);
  const storeName = store?.name ?? store?.value ?? null;

  const used = new Set(["StoreName", "Date"]);
  const columns = parsed.metrics.map((m) => {
    let name = m.label.replace(/^[-+=]\s+/, "").replace(/\s+/g, " ").replace(/\./g, "_").replace(/^\$+/, "");
    let unique = name;
    for (let n = 2; used.has(unique); n++) unique = `${name} ${n}`;
    used.add(unique);
    return { name: unique, metric: m };
  });

  const records = parsed.dates.map((date, j) => {
    const record = { StoreName: storeName, Date: date };
    for (const { name, metric } of columns) record[name] = metric.values[j];
    return record;
  });
  return { records, store };
}

// ─── Learning a profile from two sample files ──────────────────────────────────

const STOP_WORDS = new Set(["of", "and", "the", "a", "ea"]);
const tokens = (text) =>
  new Set(
    String(text)
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((t) => t && !STOP_WORDS.has(t))
      .map((t) => (t.length > 3 && t.endsWith("s") ? t.slice(0, -1) : t)),
  );
const jaccard = (a, b) => {
  if (a.size === 0 || b.size === 0) return 0;
  let shared = 0;
  for (const t of a) if (b.has(t)) shared++;
  return shared / (a.size + b.size - shared);
};
const sharedTokens = (a, b) => [...a].filter((t) => b.has(t)).length;

const fits = (sum, target, k) => {
  const tol = 0.005 * (k + 1) + 1e-6;
  for (let d = 0; d < target.length; d++) if (Math.abs(sum[d] - target[d]) > tol) return false;
  return true;
};

// Finds the smallest set of report rows whose per-day total equals the wanted
// column on every day. Additive combinations first, then with subtraction.
function solveColumn(target, rows) {
  const D = target.length;
  const n = rows.length;
  const sums = new Float64Array(D);
  const solutions = [];
  const push = (idx, signs) => {
    if (solutions.length < 40) solutions.push({ idx, signs });
  };

  const trySet = (idx, signs) => {
    sums.fill(0);
    for (let s = 0; s < idx.length; s++) {
      const v = rows[idx[s]].vec;
      for (let d = 0; d < D; d++) sums[d] += signs[s] * v[d];
    }
    if (fits(sums, target, idx.length)) push(idx, signs);
  };

  for (let a = 0; a < n; a++) trySet([a], [1]);
  if (solutions.length) return solutions;
  for (let a = 0; a < n; a++) for (let b = a + 1; b < n; b++) trySet([a, b], [1, 1]);
  if (solutions.length) return solutions;
  for (let a = 0; a < n; a++)
    for (let b = a + 1; b < n; b++) for (let c = b + 1; c < n; c++) trySet([a, b, c], [1, 1, 1]);
  if (solutions.length) return solutions;
  if (n <= 70) {
    for (let a = 0; a < n; a++)
      for (let b = a + 1; b < n; b++)
        for (let c = b + 1; c < n; c++)
          for (let e = c + 1; e < n; e++) trySet([a, b, c, e], [1, 1, 1, 1]);
    if (solutions.length) return solutions;
  }
  // differences: a - b, a + b - c, a - b - c
  for (let a = 0; a < n; a++) for (let b = 0; b < n; b++) if (a !== b) trySet([a, b], [1, -1]);
  if (solutions.length) return solutions;
  for (let a = 0; a < n; a++)
    for (let b = a + 1; b < n; b++)
      for (let c = 0; c < n; c++)
        if (c !== a && c !== b) {
          trySet([a, b, c], [1, 1, -1]);
          trySet([c, a, b], [1, -1, -1]);
        }
  return solutions;
}

// Builds a profile from a sample of the report(s) and the matching sample of
// the output format. The two samples must share at least a few days.
//   sources    : [{ rows }]  one or more report files (more weeks = better)
//   targetRows : rows of the wanted output sheet (header + one row per store/day)
export function learnProfile({ sources, targetRows, brand, name, idToName = new Map() }) {
  const warnings = [];

  // 1. the report(s): every metric with a value per day, merged across files
  const dayValues = new Map(); // key -> Map(date -> value)
  const labels = new Map(); // key -> label
  const order = [];
  let meta = {};
  let title = "";
  let parsedFirst = null;
  for (const { rows } of sources) {
    const layout = detectMetricsByDate(rows);
    if (!layout) throw new Error("A source file is not a report with metrics down the rows and days across the columns");
    const parsed = readMetricSeries(layout, rows);
    parsedFirst = parsedFirst ?? parsed;
    if (!title) title = parsed.title;
    if (Object.keys(meta).length === 0) meta = parsed.meta;
    for (const m of parsed.metrics) {
      if (!dayValues.has(m.key)) {
        dayValues.set(m.key, new Map());
        labels.set(m.key, m.label);
        order.push(m.key);
      }
      parsed.dates.forEach((d, j) => dayValues.get(m.key).set(d, m.values[j]));
    }
  }

  // 2. the output sample, read by the same parser an upload uses
  const target = extractRecords([{ sheetName: "target", rows: targetRows }], new Map(), idToName);
  if (target.records.length === 0) throw new Error("The sample output file has no readable rows (needs a store, a date and some columns)");
  const columns = target.detectedColumns.filter((c) => c !== "StoreName" && c !== "Date" && !c.startsWith("__EMPTY"));

  // 3. one store of the sample, and the days both samples cover
  const reportDates = new Set();
  for (const map of dayValues.values()) for (const d of map.keys()) reportDates.add(d);
  const byStore = new Map();
  for (const rec of target.records) {
    if (!byStore.has(rec.StoreName)) byStore.set(rec.StoreName, new Map());
    byStore.get(rec.StoreName).set(rec.Date, rec);
  }
  let targetStore = null;
  let dates = [];
  for (const [store, byDate] of byStore) {
    const shared = [...byDate.keys()].filter((d) => reportDates.has(d));
    if (shared.length > dates.length) {
      targetStore = store;
      dates = shared;
    }
  }
  if (dates.length < 3) {
    throw new Error(
      `The two samples share only ${dates.length} day(s); at least 3 are needed to work out the mapping. Use a report and an output that cover the same days.`,
    );
  }
  dates.sort();
  const records = dates.map((d) => byStore.get(targetStore).get(d));

  // 4. the report rows as vectors over those days
  const rows = order.map((key) => ({
    key,
    label: labels.get(key),
    tokens: tokens(labels.get(key)),
    vec: Float64Array.from(dates, (d) => dayValues.get(key).get(d) ?? 0),
  }));
  const isZero = (v) => v.every((x) => Math.abs(x) < 0.005);
  const nonZero = rows.filter((r) => !isZero(r.vec));
  const zeroRows = rows.filter((r) => isZero(r.vec));

  // 5. each output column
  const profileColumns = [];
  const report = [];
  const used = new Set();
  for (const colName of columns) {
    const values = records.map((r) => r[colName]);
    const nameTokens = tokens(colName);

    if (values.every((v) => v === null || v === undefined || v === "")) {
      profileColumns.push({ name: colName, blank: true });
      report.push({ column: colName, status: "blank in the sample" });
      continue;
    }
    const vec = Float64Array.from(values, (v) => (typeof v === "number" ? v : 0));

    if (isZero(vec)) {
      // nothing to measure against: only a clearly best name match is believed
      const scored = rows
        .map((r) => ({ r, s: jaccard(nameTokens, r.tokens), shared: sharedTokens(nameTokens, r.tokens) }))
        .filter((x) => x.shared >= 1 && x.s >= 0.25)
        .sort((a, b) => b.s - a.s);
      if (scored.length && scored[0].s >= 0.5 && (scored.length === 1 || scored[0].s > scored[1].s)) {
        profileColumns.push({ name: colName, add: [scored[0].r.label] });
        used.add(scored[0].r.key);
        report.push({ column: colName, status: "mapped by name only (always 0 in the sample)", add: [scored[0].r.label] });
      } else {
        profileColumns.push({ name: colName, constant: 0 });
        report.push({
          column: colName,
          status: "always 0 in the sample - left at 0",
          candidates: scored.slice(0, 3).map((x) => x.r.label),
        });
      }
      continue;
    }

    const solutions = solveColumn(vec, nonZero);
    if (solutions.length === 0) {
      profileColumns.push({ name: colName, constant: 0 });
      warnings.push(`Column "${colName}" could not be matched to any combination of report rows; it will be 0.`);
      report.push({ column: colName, status: "NOT MATCHED - left at 0" });
      continue;
    }

    // Several combinations can add up to the same numbers by coincidence. The
    // right one is the one whose rows are named like the column AND like each
    // other (the "InShop" and "Online" variants of one row), not an unrelated pair.
    const scored = solutions
      .map((s, order2) => {
        const rowTokens = s.idx.map((i) => nonZero[i].tokens);
        let cohesion = 0;
        if (rowTokens.length > 1) {
          let pairs = 0;
          for (let a = 0; a < rowTokens.length; a++)
            for (let b = a + 1; b < rowTokens.length; b++) {
              cohesion += jaccard(rowTokens[a], rowTokens[b]);
              pairs++;
            }
          cohesion /= pairs;
        }
        const nameScore = Math.max(...rowTokens.map((t) => jaccard(nameTokens, t)));
        return { ...s, score: nameScore + cohesion, order: order2 };
      })
      .sort((a, b) => b.score - a.score || a.order - b.order);
    const best = scored[0];
    const chosen = best.idx.map((i) => nonZero[i]);

    // report rows that were all zero that week but are clearly the same kind of
    // row as the chosen ones (e.g. the "MOTO" variant of "InShop"/"Online")
    const extra = [];
    if (best.signs.every((s) => s === 1)) {
      for (const z of zeroRows) {
        const closest = Math.max(...chosen.map((c) => jaccard(c.tokens, z.tokens)));
        const shared = Math.max(...chosen.map((c) => sharedTokens(c.tokens, z.tokens)));
        if (closest >= 0.5 && shared >= 2) extra.push(z);
      }
    }

    const add = [];
    const subtract = [];
    best.idx.forEach((i, k) => (best.signs[k] === 1 ? add : subtract).push(nonZero[i].label));
    for (const z of extra) add.push(z.label);
    chosen.forEach((c) => used.add(c.key));
    profileColumns.push({ name: colName, add, ...(subtract.length ? { subtract } : {}) });
    report.push({
      column: colName,
      status: scored.length > 1 && scored[1].score === best.score && scored[1].idx.length === best.idx.length
        ? "mapped (other rows give the same numbers - first one used)"
        : "mapped",
      add,
      subtract,
      alsoMatches: scored.slice(1, 4).map((s) => s.idx.map((i) => nonZero[i].label).join(" + ")),
    });
  }

  // 6. which store does the output sample's store belong to?
  const store = { metaKey: null, resolved: false };
  // the most precise header first ("Store #" before "Store Location")
  for (const want of STORE_META_KEYS) {
    for (const [key, value] of Object.entries(meta)) {
      if (normKey(key) !== want) continue;
      const found = resolveStore(value, idToName);
      if (found?.resolved && found.name === targetStore) {
        store.metaKey = key;
        store.resolved = true;
        break;
      }
    }
    if (store.resolved) break;
  }
  if (!store.resolved) {
    for (const key of Object.keys(meta)) {
      if (STORE_META_KEYS.includes(normKey(key))) {
        store.metaKey = key;
        break;
      }
    }
    warnings.push(
      `The store in the report header (${store.metaKey ?? "not found"}) is not linked to "${targetStore}" in Store Data. Add the store there so uploads get the right store name.`,
    );
  }

  const unused = nonZero.filter((r) => !used.has(r.key)).map((r) => r.label);

  const profile = {
    name,
    brands: [brand],
    layout: "metrics-by-date",
    version: 1,
    detect: { title: normKey(title), labels: order, minCoverage: 0.6 },
    store: { metaKey: store.metaKey },
    columns: profileColumns,
    learnedFrom: { dates, sampleStore: targetStore, createdAt: new Date().toISOString() },
  };

  return { profile, report: { dates, sampleStore: targetStore, columns: report, unusedReportRows: unused, store, warnings } };
}
