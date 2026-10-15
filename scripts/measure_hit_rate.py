"""Hit-rate measurement of revenue_price_gap episodes (first run: 15 Oct 2026,
result in BACKLOG.md, "Итог измерения 15 октября 2026").

Read-only: runs scripts/replay_signals.py's revenue_price_gap replay twice (on
the first-received revenue, `stored`, and on the newest complete re-fetch,
`restated`), takes the episodes that STARTED on or after FREEZE, and for 7/14/30
days after each start compares the coin's price change with the median change
of the other watchlist coins (LABELING.md rule: more than +5 pp = hit, less than
-5 pp = miss, between = too early). Baselines: the same coins outside their own
episodes, and the whole watchlist on every day. Episodes touched by known
source problems (revisions >1%, thorchain-dex 22-29 Sep, no snapshots on
2-3 Oct) are listed and tallied separately. Nothing is written anywhere.

    .venv\\Scripts\\python.exe -X utf8 scripts\\measure_hit_rate.py

Needs a complete re-fetch (scripts/refetch_daily_revenue.py) made shortly
before the run. The last price day is yesterday (today's price is intraday),
so a rerun on a later date counts more episodes at each horizon. The constants
FREEZE, TH_FAIL and GAP_DAYS are the history of the first measurement and are
deliberately not parameters.
"""
import sys, math, statistics, logging
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "scripts"))
import replay_signals as rs
from storage.db import get_connection, get_latest_complete_restated_fetch

logging.disable(logging.CRITICAL)
cfg = rs.load_config()
wl = cfg["watchlist"]["defillama_protocols"]
conn = get_connection(ROOT / "storage" / "db.sqlite")
fetched, n = get_latest_complete_restated_fetch(conn, len(wl))

FREEZE = date(2026, 9, 15)
LAST_FULL_DAY = date.today() - timedelta(days=1)   # today's price is intraday
HORIZONS = (7, 14, 30)
THR = 5.0

def run(mode):
    res = rs.replay_revenue_price_gap(conn, wl, cfg["signals"]["revenue_price_gap"],
                                      fetched if mode == "restated" else None)
    return res, rs.build_revenue_price_gap_episodes(res)

# ---- prices
prices = {}
for slug in wl:
    prices[slug] = {date.fromisoformat(r["date"]): r["price"] for r in conn.execute(
        "SELECT date, price FROM coingecko_price_history WHERE protocol_slug=? AND price>0", (slug,))}
def price_at(slug, d, max_stale=3):
    s = prices[slug]
    for k in range(max_stale + 1):
        v = s.get(d - timedelta(days=k))
        if v: return v
    return None
def chg(slug, d0, d1):
    a, b = price_at(slug, d0), price_at(slug, d1)
    return None if a is None or b is None else (b - a) / a * 100
def excess(slug, d0, h):
    d1 = d0 + timedelta(days=h)
    if d1 > LAST_FULL_DAY: return None
    c = chg(slug, d0, d1)
    if c is None: return None
    oth = [x for x in (chg(s, d0, d1) for s in wl if s != slug) if x is not None]
    if len(oth) < 10: return None
    return c - statistics.median(oth), c, statistics.median(oth)
def label(x): return "hit" if x > THR else ("miss" if x < -THR else "early")

# ---- known source problems
stored = {}
for r in conn.execute("SELECT protocol_slug, date, revenue_usd, fetched_at FROM defillama_daily_revenue ORDER BY fetched_at"):
    stored.setdefault((r["protocol_slug"], r["date"]), r["revenue_usd"])   # first received
rest = {(r["protocol_slug"], r["date"]): r["revenue_usd"] for r in conn.execute(
    "SELECT protocol_slug, date, revenue_usd FROM defillama_daily_revenue_restated WHERE fetched_at=?", (fetched,))}
flag = {}   # (slug, date) -> ratio restated/stored for >1% differences
for k, v in stored.items():
    r = rest.get(k)
    if r is None or v is None: continue
    if v == 0 and r == 0: continue
    ratio = (r / v) if v else float("inf")
    if abs(ratio - 1) > 0.01: flag[k] = ratio
TH_FAIL = (date(2026, 9, 22), date(2026, 9, 29))
GAP_DAYS = {date(2026, 10, 2), date(2026, 10, 3)}

def affected(ep):
    slug = ep.protocol_slug
    s, e = date.fromisoformat(ep.episode_start_date), date.fromisoformat(ep.episode_end_date)
    d = s; rec = set(); base = set()
    while d <= e:
        for k in range(7): rec.add(d - timedelta(days=k))
        for k in range(7, 97): base.add(d - timedelta(days=k))
        d += timedelta(days=1)
    f7 = sorted(x for x in rec if (slug, x.isoformat()) in flag)
    f90 = sorted(x for x in base if (slug, x.isoformat()) in flag)
    th7 = slug == "thorchain-dex" and any(TH_FAIL[0] <= x <= TH_FAIL[1] for x in rec)
    th90 = slug == "thorchain-dex" and any(TH_FAIL[0] <= x <= TH_FAIL[1] for x in base)
    gap7 = bool(rec & GAP_DAYS); gap90 = bool(base & GAP_DAYS)
    mx = max([abs(flag[(slug, x.isoformat())] - 1) * 100 for x in f7 + f90] or [0])
    return dict(f7=f7, f90=f90, th7=th7, th90=th90, gap7=gap7, gap90=gap90, maxdev=mx)
def is_hit_by_known(a): return bool(a["f7"] or a["f90"] or a["th7"] or a["th90"])

def pct(a, b): return f"{100*a/b:.0f}%" if b else "—"

out = {}
eps_by_mode = {}
for mode in ("stored", "restated"):
    res, eps = run(mode)
    eps_by_mode[mode] = eps
    post = [e for e in eps if date.fromisoformat(e.episode_start_date) >= FREEZE]
    ended = [e for e in eps if date.fromisoformat(e.episode_end_date) >= FREEZE]
    print(f"\n######## MODE {mode}: all-history episodes {len(eps)}, started >= 15 Sep: {len(post)}, ended >= 15 Sep: {len(ended)}")
    out[mode] = post

def episode_table(mode, eps):
    print(f"\n=== Эпизоды ({mode}), начало >= 15.09 ===")
    print("protocol | start..end | dur | ex7 ex14 ex30 (pp vs median) | known-issue flags")
    for e in sorted(eps, key=lambda x: (x.episode_start_date, x.protocol_slug)):
        s = date.fromisoformat(e.episode_start_date)
        cells = []
        for h in HORIZONS:
            x = excess(e.protocol_slug, s, h)
            cells.append("n/a" if x is None else f"{x[0]:+.1f}({label(x[0])})")
        a = affected(e)
        fl = []
        if a["f7"]: fl.append(f"rev7:{len(a['f7'])}d")
        if a["f90"]: fl.append(f"rev90:{len(a['f90'])}d")
        if a["th7"]: fl.append("TH22-29/7d")
        if a["th90"]: fl.append("TH22-29/90d")
        if a["gap7"]: fl.append("gap2-3Oct/7d")
        if a["gap90"]: fl.append("gap2-3Oct/90d")
        if fl: fl.append(f"maxdev{a['maxdev']:.0f}%")
        print(f"{e.protocol_slug} | {e.episode_start_date}..{e.episode_end_date} | {e.duration_days} | " + " ".join(cells) + " | " + (",".join(fl) or "-"))

def tally(vals):
    n = len(vals)
    if n == 0: return "n=0"
    h = sum(v > THR for v in vals); m = sum(v < -THR for v in vals); z = n - h - m
    med = statistics.median(vals)
    dec = f"; hit/(hit+miss)={pct(h, h+m)}" if h + m else ""
    return f"n={n}: hit {h} ({pct(h,n)}), miss {m} ({pct(m,n)}), early {z} ({pct(z,n)}); median excess {med:+.1f}pp{dec}"

def baseline_days(coins, eps_all, h, exclude_own):
    own = {}
    for e in eps_all:
        s, en = date.fromisoformat(e.episode_start_date), date.fromisoformat(e.episode_end_date)
        own.setdefault(e.protocol_slug, []).append((s, en))
    vals = []
    d = FREEZE
    while d + timedelta(days=h) <= LAST_FULL_DAY:
        for c in coins:
            if exclude_own and any(s <= d <= en for s, en in own.get(c, [])): continue
            x = excess(c, d, h)
            if x is not None: vals.append(x[0])
        d += timedelta(days=1)
    return vals

def report(mode, eps, subset_name, subset):
    print(f"\n--- {mode}: {subset_name} ({len(subset)} эпизодов)")
    for h in HORIZONS:
        v = [excess(e.protocol_slug, date.fromisoformat(e.episode_start_date), h) for e in subset]
        v = [x[0] for x in v if x is not None]
        print(f"  h={h:>2}d  эпизоды: {tally(v)}")
for mode in ("stored", "restated"):
    eps_all = eps_by_mode[mode]
    post = out[mode]
    episode_table(mode, post)
    coins_ep = sorted({e.protocol_slug for e in post})
    flagged = [e for e in post if is_hit_by_known(affected(e))]
    clean = [e for e in post if not is_hit_by_known(affected(e))]
    gaps = [e for e in post if affected(e)["gap7"] or affected(e)["gap90"]]
    print(f"\n##### ИТОГ {mode}: эпизодов {len(post)}, задето известными сбоями источника: {len(flagged)}, чистых: {len(clean)}; задето дырой 2–3.10: {len(gaps)}")
    report(mode, eps_all, "ВСЕ эпизоды", post)
    report(mode, eps_all, "ЧИСТЫЕ (не задеты)", clean)
    report(mode, eps_all, "ЗАДЕТЫЕ сбоями (отдельно)", flagged)
    print(f"\n  Базовый уровень ({mode}), те же дни-окна 15.09 -> :")
    for h in HORIZONS:
        b1 = baseline_days(coins_ep, post, h, True)
        b2 = baseline_days(wl, post, h, False)
        print(f"  h={h:>2}d  те же монеты ВНЕ своих эпизодов: {tally(b1)}")
        print(f"         весь список, все дни:           {tally(b2)}")

# ---- difference between two counts
print("\n\n######## РАЗНИЦА stored vs restated")
ks = {m: {(e.protocol_slug, e.episode_start_date, e.episode_end_date) for e in out[m]} for m in out}
print("stored", len(ks["stored"]), "restated", len(ks["restated"]), "одинаковых (по протоколу и датам)", len(ks["stored"] & ks["restated"]))
print("только stored:", sorted(ks["stored"] - ks["restated"]))
print("только restated:", sorted(ks["restated"] - ks["stored"]))
# live journal
rows = conn.execute("SELECT protocol_slug, date(triggered_at) d FROM signal_events WHERE signal_name='revenue_price_gap' AND date(triggered_at)>=? ORDER BY 1,2", (FREEZE.isoformat(),)).fetchall()
print("live signal_events (протокол-дней с 15.09):", len(rows))

# ---- extra: split by where the known problem sits (pre-agreed categories of BACKLOG 12 Oct)
for mode in ("stored", "restated"):
    post = out[mode]
    w7 = [e for e in post if affected(e)["f7"] or affected(e)["th7"]]
    only90 = [e for e in post if e not in w7 and (affected(e)["f90"] or affected(e)["th90"])]
    none_ = [e for e in post if e not in w7 and e not in only90]
    print(f"\n#### {mode}: окно 7д задето: {len(w7)} | только база 90д: {len(only90)} | не задето: {len(none_)}")
    for nm, sub in (("окно 7д задето", w7), ("только база 90д", only90), ("не задето", none_)):
        report(mode, post, nm, sub)
    print("  дыра 2–3.10:", [(e.protocol_slug, e.episode_start_date) for e in post if affected(e)["gap7"] or affected(e)["gap90"]])

# ---- pre-freeze starters that overlap the window
for mode in ("stored", "restated"):
    print(mode, "начались до 15.09, закончились позже:", [(e.protocol_slug, e.episode_start_date, e.episode_end_date) for e in eps_by_mode[mode]
          if date.fromisoformat(e.episode_start_date) < FREEZE <= date.fromisoformat(e.episode_end_date)])

# ---- days with incomplete collection
print("\nДни с неполным сбором снимков (<25 протоколов), 15.09–15.10:")
for r in conn.execute("SELECT substr(fetched_at,1,10) d, COUNT(DISTINCT protocol_slug) n FROM defillama_snapshots WHERE substr(fetched_at,1,10)>='2026-09-15' GROUP BY 1 HAVING n<25 ORDER BY 1"): print(" ", tuple(r))
alld = {r[0] for r in conn.execute("SELECT DISTINCT substr(fetched_at,1,10) FROM defillama_snapshots WHERE substr(fetched_at,1,10)>='2026-09-15'")}
d = FREEZE
miss = []
while d <= date(2026,10,15):
    if d.isoformat() not in alld: miss.append(d.isoformat())
    d += timedelta(days=1)
print("  дней вовсе без снимков:", miss)

# ---- reconciliation: replay(stored) fired days vs live journal
res, _ = run("stored")
rp = set()
for slug, ents in res["per_protocol_entries"].items():
    for dd, sig in ents:
        if sig and dd >= FREEZE: rp.add((slug, dd.isoformat()))
lv = {(r["protocol_slug"], r["d"]) for r in conn.execute("SELECT protocol_slug, date(triggered_at) d FROM signal_events WHERE signal_name='revenue_price_gap' AND date(triggered_at)>=?", (FREEZE.isoformat(),))}
print(f"\nСверка stored-повтора с живым журналом с 15.09: повтор {len(rp)}, журнал {len(lv)}, совпало {len(rp&lv)}, лишних в повторе {sorted(rp-lv)}, недостающих {sorted(lv-rp)}")

print("\n\n#### ДОП: общие 7 / только stored / только restated")
keyf = lambda e: (e.protocol_slug, e.episode_start_date, e.episode_end_date)
common = ks["stored"] & ks["restated"]
r_only = [e for e in out["restated"] if keyf(e) not in common]
s_only = [e for e in out["stored"] if keyf(e) not in common]
cm = [e for e in out["stored"] if keyf(e) in common]
for nm, sub in (("общие", cm), ("только stored", s_only), ("только restated", r_only)):
    report("-", None, nm, sub)

def wilson(k, n, z=1.96):
    if n == 0: return None
    p = k/n; d = 1+z*z/n; c = p+z*z/(2*n); a = z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))
    return (100*(c-a)/d, 100*(c+a)/d)
print("\nWilson 95% для доли hit:")
for mode in ("stored","restated"):
    for h in (7,14):
        v=[excess(e.protocol_slug, date.fromisoformat(e.episode_start_date), h) for e in out[mode]]
        v=[x[0] for x in v if x is not None]; k=sum(x>THR for x in v)
        w=wilson(k,len(v)); print(f"  {mode} h={h}: {k}/{len(v)} -> {w[0]:.0f}%..{w[1]:.0f}%")
print("монет в эпизодах restated:", len({e.protocol_slug for e in out['restated']}), " stored:", len({e.protocol_slug for e in out['stored']}))
# raw coin price change too (for reference)
for mode in ("stored","restated"):
    for h in (7,14):
        v=[excess(e.protocol_slug, date.fromisoformat(e.episode_start_date), h) for e in out[mode]]
        v=[x for x in v if x is not None]
        print(f"  {mode} h={h}: цена выросла у {sum(x[1]>0 for x in v)}/{len(v)}, медиана изм. монеты {statistics.median([x[1] for x in v]):+.1f}%, медиана рынка {statistics.median([x[2] for x in v]):+.1f}%")
