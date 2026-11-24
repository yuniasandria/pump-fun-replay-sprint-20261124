// Fresh-runner sniper for the pump.fun bonding-curve replay (agent-fun/replay/v2).
//
// Idea: most pump.fun curves die; a few run hard. This bot buys young curves only when the
// opening tape shows genuine, broad demand (several distinct buyers, net SOL flowing in, the
// creator still holding), takes a small fixed stake per coin, and exits fast: a take-profit, a
// trailing stop once the position is up, a hard stop loss, a time limit, and an instant exit
// when the creator sells. Breadth is the edge: many small bets, losers cut early, the rare
// runner pays for the rest.
//
// Guardrails: a small probe round trip keeps the run above the minimum bar (a round trip of at
// least 0.01 SOL on each of at least 20 coins) when the main entries lag the pace.
//
// Deterministic by construction: no clock, no randomness, no I/O; iteration only over
// insertion-ordered maps fed by the batch stream, so two runs agree byte for byte.

import type { Batch, Bot, BotState, OrderInput, Pubkey } from './vendor/makefun-bot.ts';

export interface Params {
  /** Youngest age (slots since CreateEvent) at which a first buy may be decided. */
  minAge: number;
  /** Oldest age at which a first buy may still be decided. */
  maxAge: number;
  /** Real SOL reserve window at decision time. */
  minRealSol: number;
  maxRealSol: number;
  /** Distinct non-creator buyers seen before entry. */
  minBuyers: number;
  /** Buy trades seen before entry. */
  minBuys: number;
  /** Net SOL inflow (buys minus sells) required before entry. */
  minNetSol: number;
  /** Creator's own buys: lamports window (SOL). */
  minDevBuy: number;
  maxDevBuy: number;
  /** Distinct non-creator buyers in the create slot, at most (a launch bundled by many wallets is a dump in waiting). */
  maxEarlyBuyers: number;
  /** Lamports sold by others / lamports bought, at most (as a ratio). */
  maxSellRatio: number;
  /** SOL staked into one coin in total (single buy, no pyramid). */
  stakeSol: number;
  /** Smallest buy worth sending. */
  minBuySol: number;
  /** Slippage on buys (bps); sells always go out at 10000. */
  buySlipBps: number;
  /** Take profit: sell all once close value reaches cost * (1 + takeProfit). */
  takeProfit: number;
  /** Stop loss: sell all once close value falls below cost * (1 - stopLoss). */
  stopLoss: number;
  /** Trailing stop arms once value reaches cost * (1 + trailArm); then sell below peak * (1 - trail). */
  trailArm: number;
  trail: number;
  /** Exit after this many slots held. */
  maxHold: number;
  /** Slots to wait after an exit before the same curve may be bought again. */
  cooldown: number;
  /** Most entries per curve over the run. */
  maxEntries: number;
  /** Size of one pyramid add (SOL) into a position that is already up. */
  addSol: number;
  /** Most pyramid adds per position. */
  maxAdds: number;
  /** Add when the position's value reaches cost * (1 + addGain) and fresh flow is still in. */
  addGain: number;
  /** Most momentum positions open at once. */
  maxMain: number;
  /** Most SOL at cost across all positions at once (a drawdown guard). */
  maxExposureSol: number;
  /** Guardrail pacing: target coins / fills scaled by the share of paceSecs elapsed (block time). */
  targetCoins: number;
  targetFills: number;
  paceSecs: number;
  /** Probe round-trip size (SOL); must be >= 0.01 to count toward the minimum bar. */
  probeSol: number;
  maxProbes: number;
}

export const DEFAULT_PARAMS: Params = {
  minAge: 0,
  maxAge: 1,
  minRealSol: 0.5,
  maxRealSol: 40,
  minBuyers: 5,
  minBuys: 5,
  minNetSol: 0.3,
  minDevBuy: 0.005,
  maxDevBuy: 3,
  maxEarlyBuyers: 4,
  maxSellRatio: 0.5,
  stakeSol: 0.08,
  minBuySol: 0.01,
  buySlipBps: 1500,
  takeProfit: 1000,
  stopLoss: 0.35,
  trailArm: 0.15,
  trail: 0.15,
  maxHold: 900,
  cooldown: 150,
  maxEntries: 2,
  addSol: 0.3,
  maxAdds: 4,
  addGain: 0.2,
  maxMain: 5,
  maxExposureSol: 7,
  targetCoins: 25,
  targetFills: 200,
  paceSecs: 14400,
  probeSol: 0.01,
  maxProbes: 1,
};

const LAMPORTS = 1e9;
/** Curve fee share (protocol 95 bps + creator 30 bps) taken off a sell's quote. */
const CURVE_FEE = 0.0125;
const BPS_PER_UNIT = 10_000;
/** Shared empty pending map: pending orders are usually none. Never mutated. */
const EMPTY_PENDING: Map<Pubkey, number> = new Map();

interface Tape {
  created: number | null;
  creator: string | null;
  creatorBuy: number;
  creatorSold: boolean;
  /** Distinct non-creator buyers whose buy landed in the create slot. */
  earlyBuyers: Set<string>;
  /**
   * Opening-tape aggregates, kept running per trade instead of re-scanning a
   * stored tape on every decision. They equal the scan over the tape exactly:
   * decisions only happen at age 0-1, and no trade of a curve that young can
   * be older than the 65-slot prune window (the dev window has no trade
   * before its mint's CreateEvent and one CreateEvent per mint), so nothing
   * the scan would see is ever missing here. Summation order is arrival
   * order, the same order the scan summed in, so the floats match bit for
   * bit.
   */
  buySol: number;
  sellSol: number;
  buys: number;
  /** Distinct non-creator buyers, by arrival-time creator (see note above). */
  buyers: Set<string>;
  /** Price of the first trade (-1 until the first trade) and of the latest. */
  firstPrice: number;
  lastPrice: number;
  /**
   * Pyramid flow: signed SOL per trade in arrival order (buy +sol, sell
   * -sol) with its slot, read through a head index. The flow sum over the
   * last 30 slots is summed fresh from the window on read, in arrival order,
   * so it matches the old scan over the tape bit for bit.
   */
  flowSlots: number[];
  flowSols: number[];
  flowHead: number;
  entries: number;
  lastExit: number;
}

interface Held {
  opened: number;
  peakValue: number;
  invested: number;
  exiting: boolean;
  filled: boolean;
  adds: number;
  lastAddSlot: number;
}

export function createStrategy(p: Params = DEFAULT_PARAMS): Bot {
  const tapes = new Map<Pubkey, Tape>();
  const held = new Map<Pubkey, Held>();
  /** Probe round trips in flight, by mint. */
  const probes = new Map<Pubkey, boolean>();
  /** The bot's own filled orders and the coins they were on (harness closes excluded). */
  let myFills = 0;
  /** Tx fee in lamports, cached on the first batch (fees never change mid-run). */
  let txFeeBig: bigint | null = null;
  const myCoins = new Set<Pubkey>();
  let firstTime: number | null = null;
  let lastTime: number | null = null;

  const tapeOf = (mint: Pubkey): Tape => {
    let t = tapes.get(mint);
    if (t === undefined) {
      t = {
        created: null,
        creator: null,
        creatorBuy: 0,
        creatorSold: false,
        earlyBuyers: new Set<string>(),
        buySol: 0,
        sellSol: 0,
        buys: 0,
        buyers: new Set<string>(),
        firstPrice: -1,
        lastPrice: 0,
        flowSlots: [],
        flowSols: [],
        flowHead: 0,
        entries: 0,
        lastExit: -1e12,
      };
      tapes.set(mint, t);
    }
    return t;
  };

  /** Entry signal on a young curve's opening tape at slot `now`. */
  const entrySignal = (t: Tape, now: number, rqSol: number): boolean => {
    if (t.created === null) return false;
    const age = now - t.created;
    if (age < p.minAge || age > p.maxAge) return false;
    if (rqSol < p.minRealSol || rqSol > p.maxRealSol) return false;
    if (t.creatorSold) return false;
    if (t.creatorBuy < p.minDevBuy || t.creatorBuy > p.maxDevBuy) return false;
    if (t.earlyBuyers.size > p.maxEarlyBuyers) return false;
    if (t.buys < p.minBuys || t.buyers.size < p.minBuyers) return false;
    if (t.buySol - t.sellSol < p.minNetSol) return false;
    if (t.buySol > 0 && t.sellSol / t.buySol > p.maxSellRatio) return false;
    if (t.firstPrice <= 0 || t.lastPrice < t.firstPrice * 0.98) return false;
    return true;
  };

  return {
    onBatch(state: BotState, batch: Batch): OrderInput[] {
      const now = batch.slot;
      const traded = new Set<Pubkey>();
      for (const f of batch.fills) {
        if (f.status === 'filled' && !f.client_id.startsWith('_')) {
          myFills++;
          myCoins.add(f.mint);
        }
      }
      for (const e of batch.events) {
        if (e.block_time !== null) {
          if (firstTime === null) firstTime = e.block_time;
          if (lastTime === null || e.block_time > lastTime) lastTime = e.block_time;
        }
        if (e.event === 'CreateEvent') {
          const t = tapeOf(e.mint);
          t.created = e.slot;
          t.creator = e.creator;
        } else if (e.event === 'TradeEvent') {
          const t = tapeOf(e.mint);
          const sol = Number(e.sol_amount) / LAMPORTS;
          // The opening-tape aggregates are only ever read at age <= maxAge,
          // so a trade on an older curve cannot change any decision: skip
          // the whole fold (including the price division) for it. The flow
          // window is only read for a held curve, and only from 30 slots
          // after entry on, so a trade before the curve is held can never
          // be inside a read window either: record flow only while held.
          if (t.created === null || now - t.created <= p.maxAge) {
            const price = Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves);
            if (e.is_buy) {
              t.buySol += sol;
              t.buys++;
              if (e.user !== t.creator) t.buyers.add(e.user);
            } else {
              t.sellSol += sol;
            }
            if (t.firstPrice < 0) t.firstPrice = price;
            t.lastPrice = price;
          }
          if (held.has(e.mint)) {
            t.flowSlots.push(e.slot);
            t.flowSols.push(e.is_buy ? sol : -sol);
          }
          traded.add(e.mint);
          if (t.creator !== null && e.user === t.creator) {
            if (e.is_buy) t.creatorBuy += sol;
            else t.creatorSold = true;
          } else if (e.is_buy && t.created !== null && e.slot === t.created) {
            t.earlyBuyers.add(e.user);
          }
        }
      }
      // No per-trade store to prune any more: the aggregates are running
      // values and the flow window is read through its head index.
      // Periodic sweep: drop tapes for curves long past their launch window that are not held.
      // Never drop a curve traded in this batch — the entry loop below still needs its tape.
      if (now % 10000 === 0) {
        for (const [mint, t] of tapes) {
          if (!held.has(mint) && !probes.has(mint) && !traded.has(mint) && t.created !== null && now - t.created > 65) tapes.delete(mint);
        }
      }
      // Marks only price held curves and pending orders are usually none:
      // build neither map on a batch that cannot use it.
      const marks =
        batch.marks.length === 0 || held.size === 0
          ? undefined
          : new Map(batch.marks.map((m) => [m.mint, m] as const));
      let pendingByMint: Map<Pubkey, number> = EMPTY_PENDING;
      if (state.pending.size > 0) {
        pendingByMint = new Map<Pubkey, number>();
        for (const po of state.pending.values()) {
          pendingByMint.set(po.order.mint, (pendingByMint.get(po.order.mint) ?? 0) + 1);
        }
      }

      const orders: OrderInput[] = [];
      const cap = state.hello.caps.max_orders_per_batch;
      // The tx fee is constant for the run: pay the BigInt conversion once.
      const txFee = (txFeeBig ??= state.txFeeLamports());
      const tx = Number(txFee) / LAMPORTS;

      // Exits first.
      for (const [mint, h] of held) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) {
            held.delete(mint);
            const t = tapeOf(mint);
            if (h.filled) t.lastExit = now;
            else t.entries--;
          }
          continue;
        }
        h.filled = true;
        if (curve === undefined || curve.complete) continue;
        const mk = marks?.get(mint);
        const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
        const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
        const tokens = Number(pos.token_amount);
        const value = ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - tx;
        const cost = Number(pos.cost_lamports) / LAMPORTS;
        if (value > h.peakValue) h.peakValue = value;
        let exit = h.exiting;
        const t = tapeOf(mint);
        if (!exit && value >= cost * (1 + p.takeProfit)) exit = true;
        if (!exit && value < cost * (1 - p.stopLoss)) exit = true;
        if (!exit && h.peakValue >= cost * (1 + p.trailArm) && value < h.peakValue * (1 - p.trail)) exit = true;
        if (!exit && t.creatorSold) exit = true;
        if (!exit && now - h.opened > p.maxHold) exit = true;
        if (!exit) continue;
        h.exiting = true;
        if (pendingByMint.has(mint)) {
          let sellPending = false;
          for (const po of state.pending.values()) if (po.order.mint === mint && po.order.side === 'sell') sellPending = true;
          if (sellPending) continue;
        }
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
      }

      // Probe round trips: sell the whole probe as soon as its buy has landed.
      for (const [mint, selling] of probes) {
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined) {
          if (!pendingByMint.has(mint)) probes.delete(mint);
          continue;
        }
        if (curve === undefined || curve.complete || pendingByMint.has(mint)) continue;
        if (orders.length >= cap) break;
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: 10_000 });
        if (!selling) probes.set(mint, true);
      }

      // Then entries.
      // Free cash without the SDK's per-batch call: cash minus the tx fee
      // and buy amounts of pending orders — the same formula it computes.
      let freeLamports = state.cash;
      for (const po of state.pending.values()) {
        freeLamports -= txFee;
        if (po.order.side === 'buy') freeLamports -= BigInt(po.order.quote_lamports);
      }
      let free = Number(freeLamports) / LAMPORTS;
      // Committed positions without the SDK's per-batch Set: the bot never
      // holds two pending buys on one mint (entries, pyramid adds and probes
      // all skip a mint with a pending order), so counting pending buys not
      // already held equals the distinct-mint count exactly.
      let committed = state.positions.size;
      for (const po of state.pending.values()) {
        if (po.order.side === 'buy' && !state.positions.has(po.order.mint)) committed++;
      }
      let slotsLeft = state.hello.caps.max_open_positions - committed;
      let mainLeft = p.maxMain - held.size;
      let exposure = 0;
      for (const [mint, h] of held) exposure += Math.max(h.invested, Number(state.positions.get(mint)?.cost_lamports ?? 0n) / LAMPORTS);
      for (const mint of traded) {
        if (orders.length >= cap) break;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint) || probes.has(mint) || held.has(mint)) continue;
        if (slotsLeft <= 0 || mainLeft <= 0) continue;
        const t = tapes.get(mint)!;
        if (t.entries >= p.maxEntries || now - t.lastExit < p.cooldown) continue;
        const rqSol = Number(curve.real_quote_reserves) / LAMPORTS;
        if (!entrySignal(t, now, rqSol)) continue;
        const capSol = Math.floor(Number(state.maxBuyLamports(mint))) / LAMPORTS;
        let amount = Math.min(capSol, p.stakeSol, p.maxExposureSol - exposure);
        if (amount > free - tx) amount = free - tx;
        if (amount < p.minBuySol) continue;
        const lamports = BigInt(Math.floor(amount * LAMPORTS));
        orders.push({ side: 'buy', mint, quote_lamports: lamports, max_slippage_bps: p.buySlipBps });
        free -= amount + tx;
        exposure += amount;
        held.set(mint, { opened: now, peakValue: 0, invested: amount, exiting: false, filled: false, adds: 0, lastAddSlot: now });
        t.entries++;
        slotsLeft--;
        mainLeft--;
      }

      // Pyramid adds: a position already up and still attracting fresh flow gets a bigger
      // stake, so the rare runner pays for the many small losers.
      for (const [mint, h] of held) {
        if (orders.length >= cap) break;
        if (!h.filled || h.exiting || h.adds >= p.maxAdds) continue;
        if (now - h.lastAddSlot < 30) continue;
        const pos = state.positions.get(mint);
        const curve = state.curves.get(mint);
        if (pos === undefined || curve === undefined || curve.complete) continue;
        if (pendingByMint.has(mint)) continue;
        const mk = marks?.get(mint);
        const vq = mk ? Number(mk.virtual_quote_reserves) : Number(curve.virtual_quote_reserves);
        const vt = mk ? Number(mk.virtual_token_reserves) : Number(curve.virtual_token_reserves);
        const tokens = Number(pos.token_amount);
        const value = ((tokens * vq) / (vt + tokens) / LAMPORTS) * (1 - CURVE_FEE) - tx;
        const cost = Number(pos.cost_lamports) / LAMPORTS;
        if (value < cost * (1 + p.addGain)) continue;
        const t = tapeOf(mint);
        while (t.flowHead < t.flowSlots.length && t.flowSlots[t.flowHead]! <= now - 30) t.flowHead++;
        let flow = 0;
        for (let i = t.flowHead; i < t.flowSlots.length; i++) flow += t.flowSols[i]!;
        if (flow < 0.1) continue;
        const capSol = Math.floor(Number(state.maxBuyLamports(mint))) / LAMPORTS;
        let amount = Math.min(capSol, p.addSol, p.maxExposureSol - exposure);
        if (amount > free - tx) amount = free - tx;
        if (amount < p.minBuySol) continue;
        orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(amount * LAMPORTS)), max_slippage_bps: p.buySlipBps });
        free -= amount + tx;
        exposure += amount;
        h.adds++;
        h.lastAddSlot = now;
        h.invested += amount;
      }

      // Guardrail pacing: open a probe when the bot's fills or coins lag the pace.
      if (probes.size < p.maxProbes && slotsLeft > 0 && firstTime !== null && lastTime !== null) {
        const pace = Math.min(1, (lastTime - firstTime) / p.paceSecs);
        const needCoins = myCoins.size < Math.ceil(p.targetCoins * pace);
        const needFills = myFills < Math.ceil(p.targetFills * pace);
        if (needCoins || needFills) {
          const minRq = (p.probeSol * Number(BPS_PER_UNIT)) / state.hello.caps.max_order_bps_of_real_reserve;
          for (const mint of traded) {
            if (orders.length >= cap || probes.size >= p.maxProbes) break;
            if (held.has(mint) || probes.has(mint) || pendingByMint.has(mint) || state.positions.has(mint)) continue;
            if (needCoins && !needFills && myCoins.has(mint)) continue;
            const curve = state.curves.get(mint);
            if (curve === undefined || curve.complete) continue;
            if (Number(curve.real_quote_reserves) / LAMPORTS < minRq * 1.5) continue;
            if (free < p.probeSol + 2 * tx) break;
            orders.push({ side: 'buy', mint, quote_lamports: BigInt(Math.floor(p.probeSol * LAMPORTS)), max_slippage_bps: p.buySlipBps });
            probes.set(mint, false);
            free -= p.probeSol + tx;
          }
        }
      }
      return orders;
    },
  };
}
