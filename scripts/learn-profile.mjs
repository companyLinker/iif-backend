// Learns an upload profile from a sample report and a sample of the wanted output.
//
//   node scripts/learn-profile.mjs --brand DunkinJJS --name "Weekly sales report" \
//        --target "Daily Sales Summary.xlsx" --source "Weekly report wk32.xls" \
//        [--source "Weekly report wk33.xls" ...] [--out profiles/my-profile.json] [--no-db]
//
// More source weeks make ambiguous columns (ones that were 0 in a single week)
// resolvable. Store codes are looked up in the StoreData collection (read-only);
// use --no-db to skip that. The result is a plain JSON file: review it, edit the
// column mappings if needed, and commit it under backend/profiles/.

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { MongoClient } from "mongodb";
import { readUploadedFile, loadStoreIdMap } from "../uploadProcessing.js";
import { learnProfile } from "../reportConverter.js";

const here = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(here, "..", ".env") });

const args = process.argv.slice(2);
const opts = { source: [] };
for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a === "--no-db") opts.noDb = true;
  else if (a === "--source") opts.source.push(args[++i]);
  else if (a.startsWith("--")) opts[a.slice(2)] = args[++i];
}
if (!opts.brand || !opts.target || opts.source.length === 0) {
  console.error('usage: node scripts/learn-profile.mjs --brand <Brand> --target <output sample> --source <report> [--source <report>...] [--name "..."] [--out file.json] [--no-db]');
  process.exit(1);
}

const firstSheetRows = async (file) => {
  const ext = path.extname(file).slice(1).toLowerCase();
  const sheets = await readUploadedFile(file, ext);
  return sheets[0].rows;
};

let idToName = new Map();
let client;
if (!opts.noDb && process.env.VITE_MONGO_URL) {
  client = await MongoClient.connect(process.env.VITE_MONGO_URL);
  idToName = await loadStoreIdMap(
    client.db("brandsDB").collection(opts.brand),
    client.db("transactionsDB").collection("StoreData"),
  );
}

try {
  const sources = [];
  for (const file of opts.source) sources.push({ rows: await firstSheetRows(file) });
  const { profile, report } = learnProfile({
    sources,
    targetRows: await firstSheetRows(opts.target),
    brand: opts.brand,
    name: opts.name || `${opts.brand} report`,
    idToName,
  });

  console.log(`Sample store: ${report.sampleStore} | days compared: ${report.dates.length} (${report.dates[0]} .. ${report.dates[report.dates.length - 1]})`);
  console.log(`Store link: header "${report.store.metaKey}" ${report.store.resolved ? "-> resolved through Store Data" : "(NOT resolved)"}\n`);
  for (const c of report.columns) {
    const parts = [];
    if (c.add?.length) parts.push(c.add.join(" + "));
    if (c.subtract?.length) parts.push("minus " + c.subtract.join(" minus "));
    console.log(`  ${c.column.padEnd(18)} ${c.status}${parts.length ? "  <- " + parts.join(" + ") : ""}`);
    if (c.candidates?.length) console.log(`  ${"".padEnd(18)} possible rows: ${c.candidates.join(" | ")}`);
    if (c.alsoMatches?.length) console.log(`  ${"".padEnd(18)} same numbers: ${c.alsoMatches.join(" | ")}`);
  }
  if (report.unusedReportRows.length) console.log(`\nReport rows not carried into the output format: ${report.unusedReportRows.join(", ")}`);
  for (const w of report.warnings) console.log(`\nWARNING: ${w}`);

  if (opts.out) {
    fs.mkdirSync(path.dirname(path.resolve(opts.out)), { recursive: true });
    fs.writeFileSync(opts.out, JSON.stringify(profile, null, 2) + "\n");
    console.log(`\nWrote ${opts.out}`);
  }
} finally {
  await client?.close();
}
