/**
 * Records Polymarket's "Bitcoin Up or Down" 5-minute markets next to the price they settle on, for
 * a study of whether those markets are ever mispriced (read-only: public feeds, no keys, no orders).
 *
 *   chainlink.jsonl  Chainlink BTC/USD, the settlement source (Polymarket's live data feed, ~1/s)
 *   binance.jsonl    Binance BTC/USDT (relayed by the same feed, ~1/s)
 *   coinbase.jsonl   Coinbase BTC-USD ticker (at most 4/s), when reachable
 *   book.jsonl       every change in each market's best bid/ask for Up and Down
 *   trades.jsonl     every trade (price, size, side)
 *   markets.jsonl    each market: window, slug, tokens
 *   results.jsonl    each market's outcome, once it settles
 *   alive.jsonl      connection checks, so gaps can be left out of the analysis
 *
 * Every row has `t`, this machine's clock (ms) when it arrived; feeds' own stamps are `ts`.
 *
 *   STUDY_MINUTES=200 node --experimental-strip-types record.ts
 */
import { appendFileSync, createReadStream, createWriteStream, mkdirSync, readdirSync, rmSync } from "node:fs";
import { join } from "node:path";
import { pipeline } from "node:stream/promises";
import { createGzip } from "node:zlib";

const OUT = process.env.STUDY_OUT || "data";
const END_ALL = Date.parse(process.env.STUDY_END || "2026-12-02T16:00:00Z");
const STOP = Math.min(Date.now() + Number(process.env.STUDY_MINUTES || 200) * 60_000, END_ALL);
if (Date.now() >= END_ALL) {
  console.log("the study is over");
  process.exit(0);
}
mkdirSync(OUT, { recursive: true });
const write = (f: string, o: unknown) => appendFileSync(join(OUT, f), `${JSON.stringify(o)}\n`);
const log = (m: string) => {
  console.log(`${new Date().toISOString()}  ${m}`);
  write("alive.jsonl", { t: Date.now(), k: "log", m });
};
const count: Record<string, number> = {};
const bump = (k: string) => {
  count[k] = (count[k] ?? 0) + 1;
};
let stopping = false;

/**
 * A connection that's kept alive: replaced when it's stuck connecting (15 s), silent while open
 * (30 s) or closed, without waiting on close events (they can hang for minutes on a dead network).
 */
interface Kept {
  name: string;
  ws: WebSocket;
  born: number;
  last: number;
  dead: boolean;
}
function keep(name: string, make: (k: Kept) => WebSocket, silentMs = 30_000): Kept {
  const k: Kept = { name, ws: null as unknown as WebSocket, born: Date.now(), last: 0, dead: false };
  const start = () => {
    k.born = Date.now();
    k.last = 0;
    k.ws = make(k);
  };
  start();
  const timer = setInterval(() => {
    if (k.dead) return clearInterval(timer);
    const now = Date.now();
    const s = k.ws.readyState;
    const stuck = s === 0 && now - k.born > 15_000;
    const silent = s === 1 && now - Math.max(k.last, k.born) > silentMs;
    if (stuck || silent || s === 3) {
      write("alive.jsonl", { t: now, k: "replace", name, why: stuck ? "stuck" : silent ? "silent" : "closed" });
      try {
        k.ws.close();
      } catch {}
      start();
    }
  }, 5_000);
  return k;
}
const drop = (k: Kept | null) => {
  if (!k) return;
  k.dead = true;
  try {
    k.ws.close();
  } catch {}
};

// ---------- prices: Chainlink (settlement) and Binance, via Polymarket's live data feed ----------
let rtdsLast = 0;
const rtds = keep("prices", (k) => {
  const ws = new WebSocket("wss://ws-live-data.polymarket.com");
  let ping: ReturnType<typeof setInterval> | null = null;
  ws.addEventListener("open", () => {
    write("alive.jsonl", { t: Date.now(), k: "rtds-open" });
    ws.send(
      JSON.stringify({
        action: "subscribe",
        subscriptions: [
          { topic: "crypto_prices_chainlink", type: "*", filters: '{"symbol":"btc/usd"}' },
          { topic: "crypto_prices", type: "update" },
        ],
      }),
    );
    ping = setInterval(() => ws.readyState === 1 && ws.send("PING"), 5_000);
  });
  ws.addEventListener("message", (e) => {
    if (k.ws !== ws) return;
    const t = Date.now();
    let m: any;
    try {
      m = JSON.parse(String(e.data));
    } catch {
      return;
    }
    const p = m?.payload;
    if (!p || typeof p.value !== "number") return;
    if (m.topic === "crypto_prices_chainlink" && p.symbol === "btc/usd") {
      k.last = t;
      rtdsLast = t;
      bump("chainlink");
      write("chainlink.jsonl", { t, ts: p.timestamp, p: p.value });
    } else if (m.topic === "crypto_prices" && p.symbol === "btcusdt") {
      bump("binance");
      write("binance.jsonl", { t, ts: p.timestamp, p: p.value });
    }
  });
  ws.addEventListener("close", () => {
    if (ping) clearInterval(ping);
  });
  ws.addEventListener("error", () => {});
  return ws;
});

// ---------- Coinbase ticker (best effort) ----------
let cbLast = 0;
let cbWrote = 0;
const cb = keep(
  "coinbase",
  (k) => {
    const ws = new WebSocket("wss://ws-feed.exchange.coinbase.com");
    ws.addEventListener("open", () => {
      write("alive.jsonl", { t: Date.now(), k: "coinbase-open" });
      ws.send(JSON.stringify({ type: "subscribe", product_ids: ["BTC-USD"], channels: ["ticker"] }));
    });
    ws.addEventListener("message", (e) => {
      if (k.ws !== ws) return;
      const t = Date.now();
      k.last = t;
      cbLast = t;
      if (t - cbWrote < 250) return;
      let m: any;
      try {
        m = JSON.parse(String(e.data));
      } catch {
        return;
      }
      if (m.type !== "ticker") return;
      cbWrote = t;
      bump("coinbase");
      write("coinbase.jsonl", { t, ts: Date.parse(m.time), p: Number(m.price), b: Number(m.best_bid), k: Number(m.best_ask) });
    });
    ws.addEventListener("error", () => {});
    return ws;
  },
  60_000,
);

// ---------- the 5-minute markets ----------
interface Market {
  start: number;
  end: number;
  slug: string;
  up: string;
  down: string;
  conn: Kept | null;
  settled: boolean;
  tries: number;
}
const markets = new Map<number, Market>();
const assetSide = new Map<string, { start: number; o: "up" | "down" }>();
const books = new Map<string, { bids: Map<string, number>; asks: Map<string, number>; best: string }>();

function bestOf(a: string, ts: number | null, t: number) {
  const b = books.get(a);
  const side = assetSide.get(a);
  if (!b || !side) return;
  let bid = 0;
  let ask = 1;
  for (const [p, s] of b.bids) if (s > 0 && Number(p) > bid) bid = Number(p);
  for (const [p, s] of b.asks) if (s > 0 && Number(p) < ask) ask = Number(p);
  const sig = `${bid}|${ask}`;
  if (sig === b.best) return;
  b.best = sig;
  bump("book");
  write("book.jsonl", { t, ts, m: side.start, o: side.o, b: bid, k: ask });
}
function onMarketMessage(raw: string, k: Kept) {
  const t = Date.now();
  k.last = t;
  if (raw === "PONG") return;
  let msg: any;
  try {
    msg = JSON.parse(raw);
  } catch {
    return;
  }
  for (const m of Array.isArray(msg) ? msg : [msg]) {
    const ts = m.timestamp ? Number(m.timestamp) : null;
    if (m.event_type === "book") {
      const side = (xs: any[]) => new Map<string, number>((xs ?? []).map((x) => [String(x.price), Number(x.size)]));
      books.set(m.asset_id, { bids: side(m.bids ?? m.buys), asks: side(m.asks ?? m.sells), best: "" });
      bestOf(m.asset_id, ts, t);
    } else if (m.event_type === "price_change") {
      const touched = new Set<string>();
      for (const c of m.price_changes ?? []) {
        const a = String(c.asset_id);
        let b = books.get(a);
        if (!b) {
          b = { bids: new Map(), asks: new Map(), best: "" };
          books.set(a, b);
        }
        (c.side === "BUY" ? b.bids : b.asks).set(String(c.price), Number(c.size));
        touched.add(a);
      }
      for (const a of touched) bestOf(a, ts, t);
    } else if (m.event_type === "last_trade_price") {
      const side = assetSide.get(m.asset_id);
      if (!side) continue;
      bump("trades");
      write("trades.jsonl", { t, ts, m: side.start, o: side.o, p: Number(m.price), s: Number(m.size), side: m.side });
    }
  }
}
function subscribe(mk: Market): Kept {
  return keep(`market ${mk.slug}`, (k) => {
    const ws = new WebSocket("wss://ws-subscriptions-clob.polymarket.com/ws/market");
    let ping: ReturnType<typeof setInterval> | null = null;
    ws.addEventListener("open", () => {
      write("alive.jsonl", { t: Date.now(), k: "market-open", m: mk.start });
      ws.send(JSON.stringify({ assets_ids: [mk.up, mk.down], type: "market" }));
      ping = setInterval(() => ws.readyState === 1 && ws.send("PING"), 10_000);
    });
    ws.addEventListener("message", (e) => {
      if (k.ws === ws) onMarketMessage(String(e.data), k);
    });
    ws.addEventListener("close", () => {
      if (ping) clearInterval(ping);
    });
    ws.addEventListener("error", () => {});
    return ws;
  });
}
async function gamma(slug: string): Promise<any | null> {
  try {
    const r = await fetch(`https://gamma-api.polymarket.com/events?slug=${slug}`, { signal: AbortSignal.timeout(10_000) });
    return ((await r.json()) as any[])?.[0] ?? null;
  } catch {
    return null;
  }
}
const parse = (x: unknown): string[] => {
  try {
    return typeof x === "string" ? JSON.parse(x) : ((x as string[]) ?? []);
  } catch {
    return [];
  }
};
async function ensure(start: number) {
  if (markets.has(start)) return;
  const slug = `btc-updown-5m-${start / 1000}`;
  const ev = await gamma(slug);
  const m = ev?.markets?.[0];
  if (!m) return;
  const outs = parse(m.outcomes);
  const ids = parse(m.clobTokenIds);
  const iu = outs.findIndex((o) => /up/i.test(o));
  const id = outs.findIndex((o) => /down/i.test(o));
  if (iu < 0 || id < 0 || !ids[iu] || !ids[id]) return;
  const mk: Market = { start, end: start + 300_000, slug, up: ids[iu], down: ids[id], conn: null, settled: false, tries: 0 };
  markets.set(start, mk);
  assetSide.set(mk.up, { start, o: "up" });
  assetSide.set(mk.down, { start, o: "down" });
  write("markets.jsonl", { t: Date.now(), m: start, end: mk.end, slug, up: mk.up, down: mk.down, fee: m.feeSchedule ?? null, feesEnabled: m.feesEnabled ?? null });
  mk.conn = subscribe(mk);
}
async function settle(mk: Market) {
  mk.tries++;
  const ev = await gamma(mk.slug);
  const m = ev?.markets?.[0];
  const prices = parse(m?.outcomePrices).map(Number);
  const outs = parse(m?.outcomes);
  if (!m?.closed || prices.length !== 2 || !prices.some((p) => p === 1)) return;
  const winner = outs[prices.indexOf(1)];
  mk.settled = true;
  bump("results");
  write("results.jsonl", { t: Date.now(), m: mk.start, slug: mk.slug, winner: /up/i.test(winner) ? "up" : "down" });
}

const tick = async () => {
  const now = Date.now();
  const cur = Math.floor(now / 300_000) * 300_000;
  await ensure(cur);
  await ensure(cur + 300_000);
  for (const mk of markets.values()) {
    // Watch each market until a minute after its window closes.
    if (mk.conn && now > mk.end + 60_000) {
      drop(mk.conn);
      mk.conn = null;
    }
    if (!mk.settled && now > mk.end + 60_000 && mk.tries < 40 && (now - mk.end) / 30_000 > mk.tries) await settle(mk);
  }
  write("alive.jsonl", { t: now, k: "tick", rtds: rtdsLast, coinbase: cbLast, open: [...markets.values()].filter((m) => m.conn?.ws.readyState === 1).length });
};
log(`recording until ${new Date(STOP).toISOString()}`);
const timer = setInterval(() => void tick(), 10_000);
await tick();
const beat = setInterval(() => log(`heartbeat ${JSON.stringify(count)}`), 10 * 60_000);

await new Promise((r) => setTimeout(r, Math.max(0, STOP - Date.now())));
// Markets settle about 5 minutes after they close: wait up to 8 for the last ones.
const settleBy = Date.now() + 8 * 60_000;
while (Date.now() < settleBy && [...markets.values()].some((m) => !m.settled && m.end + 60_000 < Date.now() && m.tries < 40)) {
  for (const mk of markets.values()) if (!mk.settled && mk.end + 60_000 < Date.now()) await settle(mk);
  await new Promise((r) => setTimeout(r, 30_000));
}
stopping = true;
clearInterval(timer);
clearInterval(beat);
drop(rtds);
drop(cb);
for (const mk of markets.values()) drop(mk.conn);
log(`finished ${JSON.stringify(count)}`);
for (const f of readdirSync(OUT).filter((x) => x.endsWith(".jsonl"))) {
  await pipeline(createReadStream(join(OUT, f)), createGzip(), createWriteStream(join(OUT, `${f}.gz`)));
  rmSync(join(OUT, f));
}
process.exit(0);
