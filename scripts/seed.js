const DAYS_AHEAD = 35; // at least a month

const SCHEDULE = [
  { provider: 'Dr Ade',    days: [1, 2, 3, 4, 5],    hours: [9, 9.5, 10, 10.5, 11, 11.5] }, // Mon–Fri mornings
  { provider: 'Dr Bello',  days: [1, 2, 3, 4, 5, 6], hours: [13, 13.5, 14, 14.5, 15] },     // Mon–Sat afternoons
  { provider: 'Dr Chukwu', days: [2, 4, 6],          hours: [15, 15.5, 16, 16.5] },         // Tue, Thu, Sat
];

const pad = (n) => String(n).padStart(2, '0');
const iso = (d) => d.toISOString().replace('.000Z', 'Z');

function buildSlots() {
  const slots = [];
  const soonest = Date.now() + 30 * 60000;
  const base = new Date(); base.setHours(0, 0, 0, 0);
  for (let i = 0; i < DAYS_AHEAD; i++) {
    const day = new Date(base.getFullYear(), base.getMonth(), base.getDate() + i);
    for (const doc of SCHEDULE) {
      if (!doc.days.includes(day.getDay())) continue;
      for (const hr of doc.hours) {
        const start = new Date(day.getFullYear(), day.getMonth(), day.getDate(), Math.floor(hr), (hr % 1) * 60);
        if (start.getTime() < soonest) continue;
        const end = new Date(start.getTime() + 30 * 60000);
        const date = `${start.getFullYear()}-${pad(start.getMonth() + 1)}-${pad(start.getDate())}`;
        const id = `${date}-${doc.provider.split(' ')[1].toLowerCase()}-${pad(start.getHours())}${pad(start.getMinutes())}`;
        slots.push({ id, provider: doc.provider, start: iso(start), end: iso(end) });
      }
    }
  }
  return slots;
}

(async () => {
  const url = (process.argv[2] || '').replace(/\/$/, '');
  const key = process.env.API_KEY;
  if (!/^https?:\/\//.test(url) || !key) {
    console.error('Usage: API_KEY=<key> node scripts/seed.js https://<your-api-url> --yes');
    process.exit(1);
  }
  const slots = buildSlots();
  const first = slots[0], last = slots[slots.length - 1];
  console.log(`${slots.length} slots from ${first.start} to ${last.start} for ${url}`);
  if (!process.argv.includes('--yes')) {
    console.log('This REPLACES all slots and DELETES all bookings. Re-run with --yes to continue.');
    process.exit(1);
  }
  const res = await fetch(`${url}/reset`, {
    method: 'POST',
    headers: { 'X-API-Key': key, 'Content-Type': 'application/json' },
    body: JSON.stringify({ slots }),
  });
  const body = await res.text();
  if (res.status !== 200) { console.error(`Failed: ${res.status} ${body}`); process.exit(1); }
  const check = await (await fetch(`${url}/slots`, { headers: { 'X-API-Key': key } })).json();
  console.log(`Done: ${res.status} ${body}. The API now lists ${check.slots.length} slots.`);
})().catch((e) => { console.error(e.message); process.exit(1); });
