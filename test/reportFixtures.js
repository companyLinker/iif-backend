// Synthetic stand-ins for a "report" export (metrics down the rows, one AM/PM
// column pair per day, store written in header lines, numbers as text) and for
// the flat daily table it should become. Nothing here is real data.

const fmt = (n) =>
  Number(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

export const mdY = (d) =>
  `${String(d.getMonth() + 1).padStart(2, "0")}/${String(d.getDate()).padStart(2, "0")}/${d.getFullYear()}`;

export const week = (startISO, days = 7) =>
  Array.from({ length: days }, (_, i) => {
    const d = new Date(`${startISO}T12:00:00`);
    d.setDate(d.getDate() + i);
    return d;
  });

// metrics: [{ label, values: number[] (one per day), pm?: true (value sits in the PM column) }]
export function makeReport({ store = "4582", location = "Route 1, Anytown, NJ", days, metrics, title = "Weekly Sales Report (V9)" }) {
  const rows = [
    [title],
    ["Week Ending Date:", null, mdY(days[days.length - 1])],
    ["Store Location:", null, `${store} - ${location}`],
    ["Store #", null, store],
    ["General Manager Name:", null, ""],
    [],
    [],
    ["Sales Item", "Summary", "#EA", ...days.flatMap((d, i) => [`Day - ${2 * i + 1}`, `Day - ${2 * i + 2}`])],
    [null, null, null, ...days.flatMap((d) => [mdY(d), null])],
    [null, null, null, ...days.flatMap(() => ["AM", "PM"])],
  ];
  for (const m of metrics) {
    const total = m.values.reduce((a, b) => a + b, 0);
    rows.push([
      m.label,
      fmt(total),
      0,
      ...m.values.flatMap((v) => (m.pm ? ["0.00", fmt(v)] : [fmt(v), "0.00"])),
    ]);
  }
  rows.push(["Shift Manager - Name", "", 0, ...days.flatMap(() => ["", ""])]);
  return rows;
}

// the flat "daily summary" a profile should reproduce
export function makeTarget({ storeName, days, columns }) {
  const header = ["Store Name", "Date", ...columns.map((c) => c.name)];
  const rows = [header];
  days.forEach((d, i) => {
    rows.push([storeName, mdY(d), ...columns.map((c) => (c.blank ? null : (c.values?.[i] ?? 0)))]);
  });
  return rows;
}

// deterministic pseudo-random values (so no two rows add up alike by accident)
const v = (base, k = 1) => {
  let seed = Math.round(base * 1000 * k) + 12345;
  return Array.from({ length: 7 }, () => {
    seed = (seed * 16807) % 2147483647;
    return Number(((base + (seed / 2147483647) * 120) * k).toFixed(2));
  });
};

// A report whose metrics relate to each other the way a real one does.
export function sampleMetrics() {
  const royalty = v(300);
  const tax = royalty.map((x) => Number((x * 0.0663).toFixed(2)));
  const inShop = v(80, 1);
  const online = v(40, 1);
  const amexIn = v(10, 0.5);
  const amexOnline = v(5, 0.5);
  const uber = v(30);
  const deposit = v(60);
  return {
    royalty, tax, inShop, online, amexIn, amexOnline, uber, deposit,
    list: [
      { label: "IN-Originals", values: v(90) },
      { label: "= Royalty Sales", values: royalty },
      { label: "+ Sales Tax", values: tax },
      { label: "- A/R Due CC (InShop) V/MC/D", values: inShop },
      { label: "- A/R Due CC (MOTO) V/MC/D", values: Array(7).fill(0) },
      { label: "- A/R Due CC (ONLINE) V/MC/D", values: online },
      { label: "- A/R Due CC (InShop) Amex", values: amexIn },
      { label: "- A/R Due CC (MOTO) Amex", values: Array(7).fill(0) },
      { label: "- A/R Due CC (ONLINE) Amex", values: amexOnline },
      { label: "- UberEats", values: uber },
      { label: "- Gift Cards Redeemed", values: Array(7).fill(0) },
      { label: "+ Gift Cards Issued", values: Array(7).fill(0) },
      { label: "= Expected Deposit", values: deposit },
      // a decoy equal to the tax row, living in the PM column like "PDQ" rows do
      { label: "PDQ/Mx Tax Check", values: tax, pm: true },
    ],
  };
}
