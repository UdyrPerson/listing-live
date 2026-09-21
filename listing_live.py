"""Suivi EN DIRECT de la regle listing v1 sur Hyperliquid, SANS ordre reel : annonces -> signaux -> journal fictif avec releve du carnet.

    python listing_live.py          # un passage : a lancer toutes les heures (.github/workflows/listing-live.yml)
    python listing_live.py --test   # self-check, sans reseau

Ne depend que de `requests` et de listing_feed.py. Aucune cle : API publiques seulement. Etat dans live/ (CSV, ajout seul, versionne par la tache) :
  announcements.csv  annonces retenues (memes analyseurs et meme dedoublonnage que le backtest)
  journal.csv        un signal par ligne : signal -> ouvert -> clos | stoppe, ou ineligible / entree manquee, avec le carnet releve a l'entree et a la sortie
  alerts.txt         ce qui s'est passe pendant CE passage (la tache en fait une alerte)
  health.json        nombre de passages rates de suite, par source
  equity.csv         valeur du compte a chaque passage (seulement avec une cle)
  STOP               s'il existe, plus aucune entree reelle (les sorties continuent) : arret d'urgence, a creer a la main dans le depot
Avec une cle (listing_broker.py) le meme journal porte aussi les ordres : colonnes r_*. HL_LIVE=1 envoie les ordres, sinon simulation.
Regle : research/listing-strategy-v1.md. Short a +36 h, 5 jours, stop +50 %, couverture = panier equipondere des HEDGE_N perps Hyperliquid les plus
traites (hors BTC et hors le coin), beta 1. Le but de cette etape est de MESURER : signaux manques, part tradable sur Hyperliquid, spread et
profondeur reels a l'entree, funding horaire, ecart au backtest Binance.
"""
import csv
import json
import pathlib
import sys
import time

import requests

import listing_feed as lf

LIVE = pathlib.Path("live")
INFO = "https://api.hyperliquid.xyz/info"
VENUES = {("upbit", "spot_krw"), ("bithumb", "spot_krw"), ("coinbase", "spot"), ("binance", "spot"), ("robinhood", "spot")}
DELAY_S, HOLD_S, STOP = 36 * 3600, 120 * 3600, 0.5
LATE_S = 2 * 3600                  # entree relevee plus de 2 h apres l'heure prevue : "entree manquee", pas de faux prix
LOOKBACK_S = 12 * 3600             # profondeur de relecture des fils a chaque passage (la tache GitHub peut sauter des heures)
HEDGE_N = 5
BEST_EFFORT = {"upbit"}              # l'API Upbit refuse les serveurs GitHub : les fils couvrent Upbit (rappel 96 %, precision 98 %), pas d'alerte
ALERT_AFTER = (6, 24, 72)             # nombre de passages rates de suite qui declenchent une alerte sur une source
SIZES = (1000, 5000)               # notionnels ($) pour lesquels on releve le prix executable
TAKER = 0.00045
A_COLS = ["ts", "venue", "market", "ticker", "n_tickers", "head", "source"]
SHORT_USD, REAL_LEGS, MAX_REAL = 22, 2, 4      # pilote reel : short ~22 $, couverture 2 x ~11 $ (ordre minimum Hyperliquid : 10 $), 4 positions au plus
R_COLS = ["real", "r_size", "r_px_in", "r_stop", "r_legs", "r_px_out", "r_net_usd", "r_note"]
J_COLS = ["id", "t_ann", "venue", "ticker", "coin", "status", "note", "entry_ts", "exit_ts", "t_in", "bid", "ask", "spread_bp", "px_in", "slip_1k_bp", "slip_5k_bp",
          "funding_in", "day_vol_musd", "legs", "legs_px_in", "t_out", "px_out", "funding_sum", "legs_px_out", "legs_funding", "short_net", "hedge_net", "net_hedged"] + R_COLS


# ---------------------------------------------------------------- fonctions pures

def schedule(t):
    """Annonce a t -> (entree, sortie) : meme convention que le backtest (cloture de la bougie 5 m qui suit t + 36 h)."""
    entry = (t + DELAY_S + 299) // 300 * 300 + 300
    return entry, entry + HOLD_S


def hl_coin(ticker, universe):
    """Ticker d'une annonce -> nom du perp Hyperliquid (les coins cotes par milliers s'appellent kPEPE, kBONK...), ou None."""
    t = ticker.upper()
    return t if t in universe else "k" + t if "k" + t in universe else None


def eligible(candles, t):
    """Bougies 5 m Hyperliquid -> (ok, raison). Perp preexistant (>= 800 bougies dans les 3 jours avant t) et qui traite (>= 2 clotures distinctes en 24 h)."""
    pre = [c for c in candles if t - 3 * 86400 <= c["t"] // 1000 < t]
    if len(pre) < 800:
        return False, f"perp trop recent ({len(pre)} bougies 5 m en 3 jours)"
    if len({c["c"] for c in pre if c["t"] // 1000 >= t - 86400}) < 2:
        return False, "perp fige (aucune variation en 24 h)"
    return True, ""


def exec_price(levels, usd):
    """Prix moyen obtenu en consommant un cote du carnet [{px, sz}] pour `usd` de notionnel, ou None si la profondeur affichee ne suffit pas."""
    left, qty = float(usd), 0.0
    for lv in levels:
        px, sz = float(lv["px"]), float(lv["sz"])
        take = min(left, px * sz)
        qty += take / px
        left -= take
        if left <= 1e-9:
            return usd / qty
    return None


def paper_pnl(px_in, px_out, funding_sum, legs_in, legs_out, legs_funding):
    """-> (short net, couverture nette, total) en fraction du notionnel d'entree. Le short encaisse un funding positif, le panier long le paie."""
    short = 1 - px_out / px_in - TAKER * (1 + px_out / px_in) + funding_sum
    rets = [o / i - 1 for i, o in zip(legs_in, legs_out)]
    hedge = sum(rets) / len(rets) - legs_funding - 2 * TAKER if rets else 0.0
    return short, hedge, short + hedge


# ---------------------------------------------------------------- reseau

def info(payload):
    for k in range(3):
        try:
            r = requests.post(INFO, json=payload, timeout=30)
            if r.status_code == 200:
                return r.json()
        except requests.RequestException:
            pass
        time.sleep(3 * (k + 1))
    raise RuntimeError(f"Hyperliquid indisponible : {payload.get('type')}")


def candles(coin, start, end, interval="5m"):
    return info({"type": "candleSnapshot", "req": {"coin": coin, "interval": interval, "startTime": start * 1000, "endTime": end * 1000}})


def funding_sum(coin, start, end):
    return sum(float(x["fundingRate"]) for x in info({"type": "fundingHistory", "coin": coin, "startTime": start * 1000, "endTime": end * 1000}))


def collect(now, alerts):
    """Annonces recentes des 5 lieux de la regle -> lignes A_COLS. Une source en panne est signalee, elle n'arrete pas le passage."""
    lf.TRIES = 2
    rows = []

    health_path = LIVE / "health.json"
    health = json.loads(health_path.read_text(encoding="utf-8")) if health_path.exists() else {}

    def source(name, fn):
        try:
            rows.extend(fn())
            health[name] = 0
        except (Exception, SystemExit) as e:
            health[name] = health.get(name, 0) + 1
            print(f"source en panne : {name} ({type(e).__name__}), {health[name]} passage(s) de suite")
            if health[name] in ALERT_AFTER and name not in BEST_EFFORT:                    # une source muette = des listings manques : alerte, mais pas a chaque passage
                alerts.append(f"SOURCE EN PANNE depuis {health[name]} passages : {name}")

    def upbit():
        import datetime as dt
        out = []
        for n in lf.upbit_page(1)[0]:
            got = lf.parse_upbit(n["title"])
            if got:
                out += [(int(dt.datetime.fromisoformat(n["first_listed_at"]).timestamp()), "upbit", got[0], t, len(got[1]), n["title"][:200], "upbit_api") for t in got[1]]
        return out

    def binance():
        out = []
        for a in lf.binance_page(1)[0]:
            tk = lf.parse_binance(a["title"])
            if tk:
                out += [(a["releaseDate"] // 1000, "binance", "spot", t, len(tk), a["title"][:200], "binance_cms") for t in tk]
        return out

    def wire(channel):
        def fn():
            out, before = [], None
            for _ in range(15):
                page = lf.wire_page(channel, before)
                for _, ts, text in page:
                    got = lf.parse_wire(text)
                    if got:
                        out += [(ts, got[0], got[1], t, len(got[2]), text.split("\n", 1)[0][:200], channel) for t in got[2]]
                if not page or min(p[1] for p in page) < now - LOOKBACK_S:
                    break
                before = min(p[0] for p in page)
            return out
        return fn

    source("upbit", upbit)
    source("binance", binance)
    for c in lf.WIRES:
        source(c, wire(c))
    LIVE.mkdir(exist_ok=True)
    health_path.write_text(json.dumps(health), encoding="utf-8")
    return [r for r in rows if (r[1], r[2]) in VENUES and r[0] > now - 7 * 86400]


# ---------------------------------------------------------------- etat

def read(name, cols):
    path = LIVE / name
    if not path.exists():
        return []
    with open(path, encoding="utf-8", newline="") as f:
        return [{c: r.get(c, "") for c in cols} for r in csv.DictReader(f)]


def write(name, cols, rows):
    LIVE.mkdir(exist_ok=True)
    with open(LIVE / name, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def book_snapshot(coin):
    b = info({"type": "l2Book", "coin": coin})["levels"]
    bid, ask = float(b[0][0]["px"]), float(b[1][0]["px"])
    mid = (bid + ask) / 2
    sells = [exec_price(b[0], s) for s in SIZES]                 # un short vend : il consomme les acheteurs
    buys = [exec_price(b[1], s) for s in SIZES]
    return {"bid": bid, "ask": ask, "mid": mid, "spread_bp": (ask - bid) / mid * 1e4, "sell": sells, "buy": buys}


def real_enter(j, snap, ctx, broker, journal, alerts):
    """Ordres d'entree du pilote : short, stop de protection sur l'exchange, puis les lignes de couverture. Une erreur n'interrompt jamais le journal fictif."""
    mode = "reel" if broker.live else "simulation"
    if (LIVE / "STOP").exists():
        j["r_note"] = "arret d'urgence : pas d'entree"
        return
    if sum(x["r_size"] != "" and x["r_px_out"] == "" for x in journal if x is not j) >= MAX_REAL:
        j["r_note"] = f"deja {MAX_REAL} positions : pas d'entree"
        return
    coin = j["coin"]
    if broker.live and coin in broker.state()[1]:
        j["r_note"] = "une position existe deja sur ce perp : pas d'entree"
        alerts.append(f"A VERIFIER : position inattendue sur {coin}")
        return
    try:
        broker.leverage(coin)
        size, px = broker.market(coin, "sell", broker.size_for(coin, SHORT_USD, snap["bid"]), snap["bid"])
    except Exception as e:
        j["r_note"] = f"entree en echec : {e}"
        alerts.append(f"ENTREE {mode.upper()} EN ECHEC {coin} : {e}")
        return
    j |= {"real": mode, "r_size": size, "r_px_in": px}
    try:
        j["r_stop"] = broker.stop(coin, size, px * (1 + STOP))
    except Exception as e:
        alerts.append(f"URGENT : STOP NON POSE sur {coin} ({e})")
    legs = []
    target = size * px / REAL_LEGS
    for c in json.loads(j["legs"]):
        if len(legs) == REAL_LEGS:
            break
        ref = float(ctx[c]["midPx"] or ctx[c]["markPx"])
        try:
            want = broker.size_for(c, target, ref)
            if abs(want * ref / target - 1) > 0.15:                 # pas de cotation trop gros pour ~11 $ (ZEC : 0,01 = 15 $) : ligne suivante
                continue
            broker.leverage(c)
            lsize, lpx = broker.market(c, "buy", want, ref)
            legs.append([c, lsize, lpx])
        except Exception as e:
            alerts.append(f"COUVERTURE INCOMPLETE {c} : {e}")
    j["r_legs"] = json.dumps(legs)
    alerts.append(f"ENTREE {mode.upper()} short {size} {coin} a {px} ; stop {j['r_stop'] or 'ABSENT'} ; couverture {[(c, z) for c, z, _ in legs]}")


def real_exit(j, ctx, broker, positions, alerts):
    """Rachat du short (sauf si le stop de l'exchange l'a deja fait) et vente des lignes de couverture. Relance au passage suivant en cas d'echec."""
    coin, size, px_in = j["coin"], float(j["r_size"]), float(j["r_px_in"])
    try:
        if broker.live and coin not in positions:
            px_out, j["r_note"] = px_in * (1 + STOP) * 1.005, "rachete par le stop de l'exchange (prix estime)"
        else:
            broker.cancel_all(coin)
            _, px_out = broker.market(coin, "buy", size, book_snapshot(coin)["ask"], reduce=True)
        t_in, t_out = int(j["t_in"]), int(j["t_out"])
        usd = size * (px_in - px_out) - TAKER * size * (px_in + px_out) + size * px_in * funding_sum(coin, t_in, t_out)
        for c, lsize, lpx in json.loads(j["r_legs"] or "[]"):
            _, out = broker.market(c, "sell", lsize, float(ctx[c]["midPx"] or ctx[c]["markPx"]), reduce=True)
            usd += lsize * (out - lpx) - TAKER * lsize * (out + lpx) - lsize * lpx * funding_sum(c, t_in, t_out)
    except Exception as e:
        alerts.append(f"SORTIE {j['real'].upper()} EN ECHEC {coin} : {e} -> nouvelle tentative au prochain passage")
        return
    j |= {"r_px_out": px_out, "r_net_usd": round(usd, 4)}
    alerts.append(f"SORTIE {j['real'].upper()} {coin} : {usd:+.2f} $ ({usd / (size * px_in) * 1e4:+.0f} pb du notionnel)")


def step(now, journal, new_ann, alerts, ctx, broker=None):
    """Fait avancer la machine a etats du journal. ctx = (univers Hyperliquid {nom: contexte de marche}). broker = ordres du pilote, ou None."""
    positions = broker.state()[1] if broker and broker.live else {}
    busy = {j["coin"] for j in journal if j["status"] in ("signal", "ouvert")}
    for a in new_ann:                                                           # 1. nouvelles annonces -> signal ou ineligible
        t, coin = int(a["ts"]), hl_coin(a["ticker"], ctx)
        entry, exit_ = schedule(t)
        j = {c: "" for c in J_COLS} | {"id": f"{a['venue']}-{a['ticker']}-{t}", "t_ann": t, "venue": a["venue"], "ticker": a["ticker"], "coin": coin or "", "entry_ts": entry, "exit_ts": exit_}
        recent = any(x["coin"] == coin and x["status"] != "ineligible" and abs(int(x["t_ann"]) - t) < 86400 for x in journal)
        if coin is None:
            j |= {"status": "ineligible", "note": "pas de perp sur Hyperliquid"}
        elif coin in busy or recent:
            j |= {"status": "ineligible", "note": "deja un signal ou une position sur ce perp"}
        elif now > entry + LATE_S:
            j |= {"status": "ineligible", "note": "annonce vue trop tard"}
        else:
            ok, why = eligible(candles(coin, t - 3 * 86400, t), t)
            j |= {"status": "signal" if ok else "ineligible", "note": why}
            busy |= {coin} if ok else set()
        journal.append(j)
        alerts.append(f"ANNONCE {a['venue']} {a['ticker']} -> {j['status']} {j['note']}" + (f" | entree prevue {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(entry))} sur {coin}" if j["status"] == "signal" else ""))
    for j in journal:                                                           # 2. entrees dues
        if j["status"] != "signal" or now < int(j["entry_ts"]):
            continue
        if now > int(j["entry_ts"]) + LATE_S:
            j |= {"status": "entree manquee", "note": "passage en retard de plus de 2 h"}
            alerts.append(f"ENTREE MANQUEE {j['coin']}")
            continue
        s = book_snapshot(j["coin"])
        legs = sorted((c for c in ctx if c not in ("BTC", j["coin"])), key=lambda c: -float(ctx[c].get("dayNtlVlm") or 0))[:HEDGE_N]
        px = s["sell"][0] or s["bid"]
        slip = [None if p is None else (s["mid"] - p) / s["mid"] * 1e4 for p in s["sell"]]
        j |= {"status": "ouvert", "t_in": now, "bid": s["bid"], "ask": s["ask"], "spread_bp": round(s["spread_bp"], 2), "px_in": px,
              "slip_1k_bp": "" if slip[0] is None else round(slip[0], 2), "slip_5k_bp": "" if slip[1] is None else round(slip[1], 2),
              "funding_in": ctx[j["coin"]].get("funding", ""), "day_vol_musd": round(float(ctx[j["coin"]].get("dayNtlVlm") or 0) / 1e6, 2),
              "legs": json.dumps(legs), "legs_px_in": json.dumps([float(ctx[c]["midPx"] or ctx[c]["markPx"]) for c in legs])}
        alerts.append(f"ENTREE FICTIVE short {j['coin']} a {px} (spread {s['spread_bp']:.1f} pb, glissement 1 k$ {j['slip_1k_bp']} pb, 5 k$ {j['slip_5k_bp']} pb)")
        if broker:
            real_enter(j, s, ctx, broker, journal, alerts)
    for j in journal:                                                           # 3. stops et sorties
        if j["status"] != "ouvert":
            continue
        px_in, t_in = float(j["px_in"]), int(j["t_in"])
        hit = next((c for c in candles(j["coin"], t_in, now) if float(c["h"]) >= px_in * (1 + STOP)), None)
        gone = bool(broker and broker.live and j["r_size"] != "" and j["coin"] not in positions)         # le stop de l'exchange a rachete : la couverture ne doit pas rester seule
        if hit is None and not gone and now < int(j["exit_ts"]):
            continue
        if hit is None and gone:
            t_out, px_out, status = now, px_in * (1 + STOP) * 1.005, "stoppe"
        elif hit:
            t_out, px_out, status = hit["T"] // 1000, max(float(hit["o"]), px_in * (1 + STOP)) * 1.005, "stoppe"
        else:
            s = book_snapshot(j["coin"])
            t_out, px_out, status = now, s["buy"][0] or s["ask"], "clos"
        legs, legs_in = json.loads(j["legs"]), json.loads(j["legs_px_in"])
        legs_out = []
        for c in legs:
            cs = candles(c, t_out - 3600, t_out)
            legs_out.append(float(cs[-1]["c"]) if cs else float(ctx[c]["midPx"] or ctx[c]["markPx"]))
        f_short = funding_sum(j["coin"], t_in, t_out)
        f_legs = sum(funding_sum(c, t_in, t_out) for c in legs) / len(legs) if legs else 0.0
        short, hedge, total = paper_pnl(px_in, px_out, f_short, legs_in, legs_out, f_legs)
        j |= {"status": status, "t_out": t_out, "px_out": px_out, "funding_sum": round(f_short, 6), "legs_px_out": json.dumps(legs_out), "legs_funding": round(f_legs, 6),
              "short_net": round(short, 5), "hedge_net": round(hedge, 5), "net_hedged": round(total, 5)}
        alerts.append(f"SORTIE FICTIVE ({status}) {j['coin']} : short {short * 1e4:+.0f} pb, couverture {hedge * 1e4:+.0f} pb, total {total * 1e4:+.0f} pb")
    if broker:
        for j in journal:                                                       # 4. sorties du pilote, y compris celles qui ont echoue a un passage precedent
            if j["r_size"] != "" and j["r_px_out"] == "" and j["status"] in ("clos", "stoppe"):
                real_exit(j, ctx, broker, positions, alerts)
        if broker.live:                                                         # 5. rapprochement : le compte ne doit porter que ce que le journal connait
            want = {}
            for j in journal:
                if j["r_size"] != "" and j["r_px_out"] == "":
                    want[j["coin"]] = want.get(j["coin"], 0.0) - float(j["r_size"])
                    for c, z, _ in json.loads(j["r_legs"] or "[]"):
                        want[c] = want.get(c, 0.0) + z
            have = broker.state()[1]
            bad = [c for c in set(want) | set(have) if abs(want.get(c, 0.0) - have.get(c, 0.0)) > 0.02 * max(abs(want.get(c, 0.0)), abs(have.get(c, 0.0)))]
            if bad:
                alerts.append(f"ECART ENTRE LE JOURNAL ET LE COMPTE sur {sorted(bad)} : a verifier a la main")


def main():
    now, alerts = int(time.time()), []
    known = read("announcements.csv", A_COLS)
    have = [(int(r["ts"]), r["venue"], r["market"], r["ticker"], int(r["n_tickers"]), r["head"], r["source"]) for r in known]
    merged = lf.dedup(have + [r for r in collect(now, alerts) if r not in have])          # tri par date : une annonce connue garde la priorite sur sa redite
    fresh = [dict(zip(A_COLS, r)) for r in merged if r not in have]
    meta, ctxs = info({"type": "metaAndAssetCtxs"})
    ctx = {u["name"]: c for u, c in zip(meta["universe"], ctxs) if not u.get("isDelisted")}
    journal = read("journal.csv", J_COLS)
    first_run = not (LIVE / "announcements.csv").exists()
    broker = None
    try:
        import listing_broker
        broker = listing_broker.connect()
    except Exception as e:                                                      # pas de cle, pas de ccxt, ou exchange injoignable : le suivi fictif continue
        alerts.append(f"COURTIER INDISPONIBLE ({type(e).__name__}) : suivi fictif seul")
    try:
        step(now, journal, [] if first_run else fresh, alerts, ctx, broker)     # premier passage : on amorce l'etat sans rejouer la semaine ecoulee
    except Exception as e:                                                      # le journal est modifie en place : ce qui a ete fait (ordres compris) est ecrit quand meme
        alerts.append(f"PASSAGE INTERROMPU ({type(e).__name__}) : etat sauvegarde, reprise au prochain passage")
    if broker:
        LIVE.mkdir(exist_ok=True)
        with open(LIVE / "equity.csv", "a", encoding="utf-8") as f:
            f.write(f"{now},{broker.state()[0]:.2f},{'reel' if broker.live else 'simulation'}\n")
    write("announcements.csv", A_COLS, [dict(zip(A_COLS, r)) for r in merged])
    write("journal.csv", J_COLS, journal)
    (LIVE / "alerts.txt").write_text("\n".join(alerts), encoding="utf-8")
    print(f"{time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(now))} | annonces connues {len(merged)} (+{len(fresh)}) | journal {len(journal)} lignes | "
          f"ouverts {sum(j['status'] == 'ouvert' for j in journal)} | signaux en attente {sum(j['status'] == 'signal' for j in journal)} | "
          f"courtier {'absent' if not broker else 'REEL' if broker.live else 'simulation'}")
    print("\n".join(alerts))


def _selftest():
    assert schedule(1000000000) == (1000000000 + DELAY_S + 200 + 300, 1000000000 + DELAY_S + 500 + HOLD_S)         # 1e9 + 36 h n'est pas sur une bougie : arrondi au-dessus
    assert schedule(1000000000)[0] % 300 == 0 and schedule(999999900) == (999999900 + DELAY_S + 300, 999999900 + DELAY_S + 300 + HOLD_S)   # deja sur une bougie : pas d'arrondi
    uni = {"SUI": {}, "kPEPE": {}, "BTC": {}}
    assert hl_coin("SUI", uni) == "SUI" and hl_coin("PEPE", uni) == "kPEPE" and hl_coin("ZAMA", uni) is None
    t = 2000000000
    good = [{"t": (t - 3 * 86400 + 300 * i) * 1000, "c": str(1 + i % 7)} for i in range(864)]
    assert eligible(good, t) == (True, "")
    assert eligible(good[:500], t)[0] is False and eligible([c | {"c": "1"} for c in good], t) == (False, "perp fige (aucune variation en 24 h)")
    bids = [{"px": "100", "sz": "5"}, {"px": "99", "sz": "10"}]                                  # 500 $ a 100 puis 990 $ a 99
    assert exec_price(bids, 500) == 100 and abs(exec_price(bids, 995) - 995 / (5 + 5)) < 1e-9 and exec_price(bids, 5000) is None
    s, h, tot = paper_pnl(100, 90, 0.002, [10, 20], [11, 20], 0.001)
    assert abs(s - (0.10 - TAKER * 1.9 + 0.002)) < 1e-12 and abs(h - (0.05 - 0.001 - 2 * TAKER)) < 1e-12 and abs(tot - s - h) < 1e-12
    journal, alerts = [], []                                                                      # machine a etats, sans reseau
    step(t, journal, [{"ts": t - 3600, "venue": "upbit", "ticker": "ZAMA"}], alerts, uni)
    assert journal[0]["status"] == "ineligible" and "Hyperliquid" in journal[0]["note"]
    step(t, journal, [{"ts": t - 40 * 3600, "venue": "upbit", "ticker": "SUI"}], alerts, uni)     # vue 40 h apres : trop tard pour une entree a +36 h
    assert journal[1]["note"] == "annonce vue trop tard" and len(alerts) == 2

    class Fake:                                                                                   # courtier sans reseau : enregistre les ordres
        live, sent = False, []
        def state(self): return 150.0, {}
        def size_for(self, coin, usd, px): return round(usd / px, 4)
        def leverage(self, coin): pass
        def market(self, coin, side, size, ref, reduce=False):
            self.sent.append((coin, side, size, reduce))
            return size, ref
        def stop(self, coin, size, trigger): return f"stop@{trigger:.2f}"
        def cancel_all(self, coin): pass
    b, ctx2 = Fake(), {"ETH": {"midPx": "2000"}, "SOL": {"midPx": "100"}, "HYPE": {"midPx": "50"}}
    j = {c: "" for c in J_COLS} | {"coin": "SUI", "legs": json.dumps(["ETH", "SOL", "HYPE"]), "status": "ouvert"}
    real_enter(j, {"bid": 1.0}, ctx2, b, [j], alerts)
    assert j["real"] == "simulation" and j["r_size"] == 22.0 and j["r_stop"] == "stop@1.50" and [x[:2] for x in b.sent] == [("SUI", "sell"), ("ETH", "buy"), ("SOL", "buy")]
    assert [round(z * p, 6) for _, z, p in json.loads(j["r_legs"])] == [11.0, 11.0]                # couverture = notionnel du short, en 2 lignes
    full = [{c: "" for c in J_COLS} | {"r_size": "1"} for _ in range(MAX_REAL)]
    k = {c: "" for c in J_COLS} | {"coin": "SUI", "legs": "[]"}
    real_enter(k, {"bid": 1.0}, ctx2, b, full + [k], alerts)
    assert k["r_size"] == "" and "positions" in k["r_note"]                                       # plafond de positions respecte
    print("self-check OK")


if __name__ == "__main__":
    _selftest() if "--test" in sys.argv else main()
