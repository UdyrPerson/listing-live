"""Suivi EN DIRECT de la regle listing v1 : annonces -> signaux -> journal, avec le carnet releve a l'entree et a la sortie.

    python listing_live.py          # un passage : a lancer toutes les heures (.github/workflows/listing-live.yml du depot de deploiement)
    python listing_live.py --test   # self-check, sans reseau

Sans cle : journal FICTIF seul (API publiques, requests + listing_feed.py). Avec une cle (listing_broker.py) le meme journal porte aussi les ordres du
pilote sur Hyperliquid : colonnes r_*. HL_LIVE=1 envoie les ordres, sinon simulation. Exchange d'un signal : Hyperliquid si le perp y existe (le moins
cher), sinon Aster, en FICTIF SEULEMENT (research/execution-venues.md). Etat dans live/ (CSV versionnes par la tache) :
  announcements.csv  annonces retenues (memes analyseurs et meme dedoublonnage que le backtest)
  journal.csv        un signal par ligne : signal -> ouvert -> clos | stoppe, ou ineligible / entree manquee
  alerts.txt         ce qui s'est passe pendant CE passage (la tache en fait une issue)
  health.json        nombre de passages rates de suite, par source
  markets.json       tickers en won cotes sur Upbit et Bithumb au dernier passage (detecteur de listing de secours)
  equity.csv         valeur du compte a chaque passage (seulement avec une cle)
  STOP               s'il existe, plus aucune entree reelle (les sorties continuent) : arret d'urgence, a creer a la main dans le depot
Regle : research/listing-strategy-v1.md. Short a +36 h, 5 jours, stop +50 %, couverture = panier equipondere des HEDGE_N perps Hyperliquid les plus
traites (hors BTC, hors le coin, hors coins shortes par ailleurs), beta 1. Pilote reel : short ~SHORT_USD, REAL_LEGS lignes de couverture.
"""
import csv
import json
import pathlib
import re
import sys
import time

import requests

import listing_feed as lf

LIVE = pathlib.Path("live")
INFO = "https://api.hyperliquid.xyz/info"
ASTER = "https://fapi.asterdex.com"
VENUES = {("upbit", "spot_krw"), ("bithumb", "spot_krw"), ("coinbase", "spot"), ("binance", "spot"), ("robinhood", "spot")}
DELAY_S, HOLD_S, STOP = 36 * 3600, 120 * 3600, 0.5
LATE_S = 6 * 3600                  # tolerance de retard a l'entree. La tache GitHub ne delivre que ~1 passage sur 4 (trous jusqu'a 4,6 h) ; le backtest montre
                                   # un plateau du delai d'entree (+36 h a +60 h : +259 a +328 pb couvert), donc entrer tard vaut bien mieux que manquer le trade.
LOOKBACK_S = 12 * 3600             # profondeur de relecture des fils a chaque passage (la tache GitHub peut sauter des heures)
HEDGE_N = 5
BEST_EFFORT = {"upbit", "bithumb"}  # sources redondantes (les fils couvrent Upbit a 96 % et Bithumb a 97 %) : une panne ne merite pas d'alerte
ALERT_AFTER = (6, 24, 72)          # nombre de passages rates de suite qui declenchent une alerte sur une source
SIZES = (1000, 5000)               # notionnels ($) pour lesquels on releve le prix executable
TAKER = {"hl": 0.00045, "aster": 0.00035}
SHORT_USD, REAL_LEGS, MAX_REAL = 22, 2, 4      # pilote reel : short ~22 $, couverture 2 x ~11 $ (ordre minimum Hyperliquid : 10 $), 4 positions au plus
SPRT_DRIFT, SPRT_BOUND = 0.02, 1.95            # test sequentiel de Wald sur le resultat couvert par trade (research/listing-strategy-v1.md)
MAX_NEW_MARKETS = 8                # plus de 8 marches neufs d'un coup = etat perdu, pas 8 listings : on reamorce sans emettre
KEY_WARN_DAYS = 14
A_COLS = ["ts", "venue", "market", "ticker", "n_tickers", "head", "source"]
R_COLS = ["real", "r_size", "r_px_in", "r_stop", "r_legs", "r_px_out", "r_legs_out", "r_net_usd", "r_note"]
J_COLS = ["id", "t_ann", "venue", "ticker", "exch", "coin", "status", "note", "entry_ts", "exit_ts", "t_in", "bid", "ask", "spread_bp", "px_in", "slip_1k_bp", "slip_5k_bp",
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


def aster_bases(symbols):
    """Symboles Aster negociables -> {ticker: symbole} (1000PEPEUSDT -> PEPE)."""
    return {re.sub(r"^(1000000|10000|1000|1M)", "", s[:-4]): s for s in symbols if s.endswith("USDT")}


def eligible(candles, t):
    """Bougies 5 m -> (ok, raison). Perp preexistant (>= 800 bougies dans les 3 jours avant t) et qui traite (>= 2 clotures distinctes en 24 h)."""
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


def paper_pnl(px_in, px_out, funding_sum, legs_in, legs_out, legs_funding, taker=TAKER["hl"]):
    """-> (short net, couverture nette, total) en fraction du notionnel d'entree. Le short encaisse un funding positif, le panier long le paie."""
    short = 1 - px_out / px_in - taker * (1 + px_out / px_in) + funding_sum
    rets = [o / i - 1 for i, o in zip(legs_in, legs_out)]
    hedge = sum(rets) / len(rets) - legs_funding - 2 * TAKER["hl"] if rets else 0.0
    return short, hedge, short + hedge


def sprt(values):
    """Resultats couverts par trade (fractions) -> (n, S, verdict). Valider si S >= 0,02 n + 1,95 ; arreter si S <= 0,02 n - 1,95."""
    n, s = len(values), sum(values)
    return n, s, "VALIDE" if s >= SPRT_DRIFT * n + SPRT_BOUND else "ARRET" if s <= SPRT_DRIFT * n - SPRT_BOUND else "en cours"


def expected(journal):
    """Positions que le compte DOIT porter d'apres le journal : {coin: taille signee}. Short tant qu'il n'est pas rachete, lignes tant qu'elles ne sont pas vendues."""
    want = {}
    for j in journal:
        if j["r_size"] == "" or j["r_net_usd"] != "":
            continue
        if j["r_px_out"] == "":
            want[j["coin"]] = want.get(j["coin"], 0.0) - float(j["r_size"])
        sold = {c for c, _ in json.loads(j["r_legs_out"] or "[]")}
        for c, z, _ in json.loads(j["r_legs"] or "[]"):
            if c not in sold:
                want[c] = want.get(c, 0.0) + z
    return {c: z for c, z in want.items() if abs(z) > 1e-12}


# ---------------------------------------------------------------- reseau (API publiques)

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


def aster(path, **params):
    seen = []
    for k in range(4):
        try:
            r = requests.get(ASTER + path, params=params, headers=lf.UA, timeout=30)
            if r.status_code == 200:
                return r.json()
            seen.append(r.status_code)
        except requests.RequestException as e:
            seen.append(type(e).__name__)
        time.sleep(5 * (k + 1))                                     # 429 / 418 : on laisse passer la fenetre de debit
    raise RuntimeError(f"Aster indisponible : {path} {seen}")


def aster_symbols():
    return [s["symbol"] for s in aster("/fapi/v1/exchangeInfo")["symbols"] if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL"]


def candles(exch, coin, start, end):
    """Bougies 5 m [{t, T (ms), o, h, c}] : meme forme pour les deux exchanges."""
    if exch == "aster":
        return [{"t": r[0], "T": r[6], "o": r[1], "h": r[2], "c": r[4]} for r in aster("/fapi/v1/klines", symbol=coin, interval="5m", startTime=start * 1000, endTime=end * 1000, limit=1500)]
    return info({"type": "candleSnapshot", "req": {"coin": coin, "interval": "5m", "startTime": start * 1000, "endTime": end * 1000}})


def funding_sum(exch, coin, start, end):
    if exch == "aster":
        return sum(float(x["fundingRate"]) for x in aster("/fapi/v1/fundingRate", symbol=coin, startTime=start * 1000, endTime=end * 1000, limit=1000))
    return sum(float(x["fundingRate"]) for x in info({"type": "fundingHistory", "coin": coin, "startTime": start * 1000, "endTime": end * 1000}))


def book_snapshot(exch, coin):
    if exch == "aster":
        d = aster("/fapi/v1/depth", symbol=coin, limit=100)
        b = [[{"px": p, "sz": q} for p, q in d["bids"]], [{"px": p, "sz": q} for p, q in d["asks"]]]
    else:
        b = info({"type": "l2Book", "coin": coin})["levels"]
    bid, ask = float(b[0][0]["px"]), float(b[1][0]["px"])
    mid = (bid + ask) / 2
    return {"bid": bid, "ask": ask, "mid": mid, "spread_bp": (ask - bid) / mid * 1e4,
            "sell": [exec_price(b[0], s) for s in SIZES], "buy": [exec_price(b[1], s) for s in SIZES]}      # un short vend : il consomme les acheteurs


def collect(now, alerts):
    """Annonces recentes des 5 lieux de la regle -> lignes A_COLS. Une source en panne est signalee, elle n'arrete pas le passage."""
    import datetime as dt
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
            if health[name] in ALERT_AFTER and name not in BEST_EFFORT:     # une source muette = des listings manques : alerte, mais pas a chaque passage
                alerts.append(f"SOURCE EN PANNE depuis {health[name]} passages : {name}")

    def upbit():
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

    def bithumb():
        out = []
        for n in lf.bithumb_page()[0]:
            tk = lf.parse_bithumb(n["title"])
            if tk:
                out += [(lf.kst_epoch(n["published_at"]), "bithumb", "spot_krw", t, len(tk), n["title"][:200], "bithumb_api") for t in tk]
        return out

    def markets(venue):
        """Nouveau marche en won apparu depuis le dernier passage = listing. Detection a l'OUVERTURE des echanges, donc un peu apres l'annonce :
        acceptable (le backtest montre un plateau du delai d'entree), et c'est la seule voie quand l'API d'avis est bloquee."""
        def fn():
            path = LIVE / "markets.json"
            known = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
            seed = venue not in known                                # premier passage pour ce lieu : on amorce l'etat sans rien emettre
            cur = sorted(lf.krw_markets(venue))
            fresh = sorted(set(cur) - set(known.get(venue, [])))
            known[venue] = cur
            LIVE.mkdir(exist_ok=True)
            path.write_text(json.dumps(known), encoding="utf-8")
            if seed or len(fresh) > MAX_NEW_MARKETS:                 # trop de nouveaux d'un coup = etat perdu, pas une vague de listings
                return []
            return [(now, venue, "spot_krw", t, 1, f"nouveau marche {t}/KRW detecte sur {venue}", venue + "_markets") for t in fresh]
        return fn

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
    for v in ("upbit", "bithumb"):
        source(f"marches {v}", markets(v))   # detection par apparition d'un marche en won : seule voie pour Upbit depuis GitHub (avis = 403)
    source("bithumb", bithumb)          # API officielle : 5 derniers avis seulement, donc redondante avec les fils, pas un remplacement
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


# ---------------------------------------------------------------- ordres du pilote (Hyperliquid seulement)

def real_enter(j, snap, ctx, broker, journal, alerts):
    """Short, stop de protection sur l'exchange, puis les lignes de couverture. Une erreur n'interrompt jamais le journal fictif."""
    mode, coin = "reel" if broker.live else "simulation", j["coin"]
    if (LIVE / "STOP").exists():
        j["r_note"] = "arret d'urgence : pas d'entree"
        return
    if sum(x["r_size"] != "" and x["r_net_usd"] == "" for x in journal if x is not j) >= MAX_REAL:
        j["r_note"] = f"deja {MAX_REAL} positions : pas d'entree"
        return
    if coin in expected(journal) or (broker.live and coin in broker.state()[1]):
        j["r_note"] = "ce perp est deja en portefeuille (position ou ligne de couverture) : pas d'entree"      # Hyperliquid compense les positions d'un meme coin
        alerts.append(f"ENTREE {mode.upper()} ECARTEE {coin} : perp deja en portefeuille")
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
        alerts.append(f"URGENT : STOP NON POSE sur {coin} ({e}) -> nouvelle tentative au prochain passage")
    legs, target = [], size * px / REAL_LEGS
    for c in json.loads(j["legs"]):                                 # deja hors BTC, hors le coin, hors coins shortes par ailleurs
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
    """Rachat du short puis vente des lignes, etape par etape : chaque etape reussie est ecrite, un nouvel essai ne refait jamais une etape deja faite."""
    coin, size, px_in = j["coin"], float(j["r_size"]), float(j["r_px_in"])
    try:
        if j["r_px_out"] == "":
            if broker.live and coin not in positions:
                j |= {"r_px_out": px_in * (1 + STOP) * 1.005, "r_note": "rachete par le stop de l'exchange (prix estime)"}
            else:
                broker.cancel_all(coin)
                j["r_px_out"] = broker.market(coin, "buy", size, book_snapshot("hl", coin)["ask"], reduce=True)[1]
        sold = json.loads(j["r_legs_out"] or "[]")
        for c, lsize, _ in json.loads(j["r_legs"] or "[]"):
            if c not in {x[0] for x in sold}:
                sold.append([c, broker.market(c, "sell", lsize, float(ctx[c]["midPx"] or ctx[c]["markPx"]), reduce=True)[1]])
                j["r_legs_out"] = json.dumps(sold)
        px_out, out, t_in, t_out = float(j["r_px_out"]), dict(sold), int(j["t_in"]), int(j["t_out"])
        usd = size * (px_in - px_out) - TAKER["hl"] * size * (px_in + px_out) + size * px_in * funding_sum("hl", coin, t_in, t_out)
        for c, lsize, lpx in json.loads(j["r_legs"] or "[]"):
            usd += lsize * (out[c] - lpx) - TAKER["hl"] * lsize * (out[c] + lpx) - lsize * lpx * funding_sum("hl", c, t_in, t_out)
    except Exception as e:
        alerts.append(f"SORTIE {j['real'].upper()} INCOMPLETE {coin} : {e} -> suite au prochain passage")
        return
    j["r_net_usd"] = round(usd, 4)
    alerts.append(f"SORTIE {j['real'].upper()} {coin} : {usd:+.2f} $ ({usd / (size * px_in) * 1e4:+.0f} pb du notionnel)")


# ---------------------------------------------------------------- machine a etats

def step(now, journal, new_ann, alerts, ctx, aster_syms=None, broker=None):
    """Fait avancer le journal. ctx = {perp Hyperliquid: contexte de marche} ; aster_syms = {ticker: symbole Aster} ; broker = ordres du pilote, ou None."""
    live = bool(broker and broker.live)
    positions = broker.state()[1] if live else {}                   # etat du compte AVANT ce passage : ne sert qu'aux positions ouvertes a un passage precedent
    busy = {j["ticker"] for j in journal if j["status"] in ("signal", "ouvert")}
    for a in new_ann:                                               # 1. nouvelles annonces -> signal ou ineligible
        t, tick = int(a["ts"]), a["ticker"].upper()
        coin = hl_coin(tick, ctx)
        exch, coin = ("hl", coin) if coin else ("aster", (aster_syms or {}).get(tick))
        entry, exit_ = schedule(t)
        j = {c: "" for c in J_COLS} | {"id": f"{a['venue']}-{tick}-{t}", "t_ann": t, "venue": a["venue"], "ticker": tick, "exch": exch if coin else "", "coin": coin or "",
                                       "entry_ts": entry, "exit_ts": exit_}
        recent = any(x["ticker"] == tick and x["status"] != "ineligible" and abs(int(x["t_ann"]) - t) < 86400 for x in journal)
        if coin is None:
            j |= {"status": "ineligible", "note": "pas de perp sur Hyperliquid ni sur Aster" if aster_syms else "pas de perp sur Hyperliquid (Aster injoignable)"}
        elif tick in busy or recent:
            j |= {"status": "ineligible", "note": "deja un signal ou une position sur ce coin"}
        elif now > entry + LATE_S:
            j |= {"status": "ineligible", "note": "annonce vue trop tard"}
        else:
            ok, why = eligible(candles(exch, coin, t - 3 * 86400, t), t)
            j |= {"status": "signal" if ok else "ineligible", "note": why}
            busy |= {tick} if ok else set()
        journal.append(j)
        alerts.append(f"ANNONCE {a['venue']} {tick} -> {j['status']} {j['note']}" + (f" | entree prevue {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(entry))} sur {exch}:{coin}" if j["status"] == "signal" else ""))
    for j in journal:                                               # 2. entrees dues
        if j["status"] != "signal" or now < int(j["entry_ts"]):
            continue
        if now > int(j["entry_ts"]) + LATE_S:
            j |= {"status": "entree manquee", "note": f"passage en retard de plus de {LATE_S // 3600} h"}
            alerts.append(f"ENTREE MANQUEE {j['coin']}")
            continue
        s = book_snapshot(j["exch"], j["coin"])
        shorted = {x["coin"] for x in journal if x["exch"] == "hl" and x["status"] in ("signal", "ouvert")} | {hl_coin(j["ticker"], ctx)}
        legs = sorted((c for c in ctx if c != "BTC" and c not in shorted), key=lambda c: -float(ctx[c].get("dayNtlVlm") or 0))[:HEDGE_N]
        px = s["sell"][0] or s["bid"]
        slip = [None if p is None else (s["mid"] - p) / s["mid"] * 1e4 for p in s["sell"]]
        vol = float(ctx[j["coin"]].get("dayNtlVlm") or 0) / 1e6 if j["exch"] == "hl" else ""
        # exit_ts est recalcule ici : la tenue de 5 jours part de l'entree REELLE, comme dans le backtest, meme si le passage horaire est en retard
        j |= {"status": "ouvert", "t_in": now, "exit_ts": now + HOLD_S, "bid": s["bid"], "ask": s["ask"], "spread_bp": round(s["spread_bp"], 2), "px_in": px,
              "slip_1k_bp": "" if slip[0] is None else round(slip[0], 2), "slip_5k_bp": "" if slip[1] is None else round(slip[1], 2),
              "funding_in": ctx[j["coin"]].get("funding", "") if j["exch"] == "hl" else "", "day_vol_musd": "" if vol == "" else round(vol, 2),
              "legs": json.dumps(legs), "legs_px_in": json.dumps([float(ctx[c]["midPx"] or ctx[c]["markPx"]) for c in legs])}
        alerts.append(f"ENTREE FICTIVE short {j['exch']}:{j['coin']} a {px} (spread {s['spread_bp']:.1f} pb, glissement 1 k$ {j['slip_1k_bp']} pb, 5 k$ {j['slip_5k_bp']} pb)")
        if broker and j["exch"] == "hl":
            real_enter(j, s, ctx, broker, journal, alerts)
    for j in journal:                                               # 3. stops et sorties
        if j["status"] != "ouvert":
            continue
        px_in, t_in = float(j["px_in"]), int(j["t_in"])
        if t_in >= now:                                             # ouverte a CE passage : rien n'a pu se passer, et `positions` (lu avant l'entree) ne la connait pas
            continue
        held = live and j["r_size"] != ""                           # position reelle ouverte a un passage precedent : `positions` la connait forcement
        if held and j["r_stop"] == "" and j["coin"] in positions:   # stop absent (pose en echec) : on le repose
            try:
                j["r_stop"] = broker.stop(j["coin"], float(j["r_size"]), float(j["r_px_in"]) * (1 + STOP))
                alerts.append(f"STOP REPOSE sur {j['coin']}")
            except Exception as e:
                alerts.append(f"URGENT : STOP TOUJOURS ABSENT sur {j['coin']} ({e})")
        hit = next((c for c in candles(j["exch"], j["coin"], t_in, now) if float(c["h"]) >= px_in * (1 + STOP)), None)
        gone = held and j["coin"] not in positions                  # le stop de l'exchange a rachete : la couverture ne doit pas rester seule
        if hit is None and not gone and now < int(j["exit_ts"]):
            continue
        if hit is None and gone:
            t_out, px_out, status = now, px_in * (1 + STOP) * 1.005, "stoppe"
        elif hit:
            t_out, px_out, status = hit["T"] // 1000, max(float(hit["o"]), px_in * (1 + STOP)) * 1.005, "stoppe"
        else:
            s = book_snapshot(j["exch"], j["coin"])
            t_out, px_out, status = now, s["buy"][0] or s["ask"], "clos"
        legs, legs_in = json.loads(j["legs"]), json.loads(j["legs_px_in"])
        legs_out = []
        for c in legs:
            cs = candles("hl", c, t_out - 3600, t_out)
            legs_out.append(float(cs[-1]["c"]) if cs else float(ctx[c]["midPx"] or ctx[c]["markPx"]))
        f_short = funding_sum(j["exch"], j["coin"], t_in, t_out)
        f_legs = sum(funding_sum("hl", c, t_in, t_out) for c in legs) / len(legs) if legs else 0.0
        short, hedge, total = paper_pnl(px_in, px_out, f_short, legs_in, legs_out, f_legs, TAKER[j["exch"]])
        j |= {"status": status, "t_out": t_out, "px_out": px_out, "funding_sum": round(f_short, 6), "legs_px_out": json.dumps(legs_out), "legs_funding": round(f_legs, 6),
              "short_net": round(short, 5), "hedge_net": round(hedge, 5), "net_hedged": round(total, 5)}
        n, S, verdict = sprt([float(x["net_hedged"]) for x in journal if x["net_hedged"] != ""])
        alerts.append(f"SORTIE FICTIVE ({status}) {j['exch']}:{j['coin']} : short {short * 1e4:+.0f} pb, couverture {hedge * 1e4:+.0f} pb, total {total * 1e4:+.0f} pb | "
                      f"test sequentiel : n {n}, S {S:+.3f}, bornes [{SPRT_DRIFT * n - SPRT_BOUND:+.2f} ; {SPRT_DRIFT * n + SPRT_BOUND:+.2f}] -> {verdict}")
    if broker:
        for j in journal:                                           # 4. sorties du pilote, y compris celles restees incompletes a un passage precedent
            if j["r_size"] != "" and j["r_net_usd"] == "" and j["status"] in ("clos", "stoppe"):
                real_exit(j, ctx, broker, positions, alerts)
        if live:                                                    # 5. rapprochement, sur l'etat du compte APRES les ordres de ce passage
            want, have = expected(journal), broker.state()[1]
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
    try:
        aster_syms = aster_bases(aster_symbols())
    except Exception as e:                                                      # Aster n'est qu'un releve fictif : son absence ne bloque rien
        print(f"Aster injoignable ({type(e).__name__})")
        aster_syms = {}
    journal = read("journal.csv", J_COLS)
    first_run = not (LIVE / "announcements.csv").exists()
    broker = None
    try:
        import listing_broker
        broker = listing_broker.connect()
    except Exception as e:                                                      # pas de cle, pas de ccxt, ou exchange injoignable : le suivi fictif continue
        alerts.append(f"COURTIER INDISPONIBLE ({type(e).__name__}) : suivi fictif seul")
    try:
        step(now, journal, [] if first_run else fresh, alerts, ctx, aster_syms, broker)     # premier passage : on amorce l'etat sans rejouer la semaine ecoulee
    except Exception as e:                                                      # le journal est modifie en place : ce qui a ete fait (ordres compris) est ecrit quand meme
        alerts.append(f"PASSAGE INTERROMPU ({type(e).__name__}) : etat sauvegarde, reprise au prochain passage")
    if broker:
        LIVE.mkdir(exist_ok=True)
        with open(LIVE / "equity.csv", "a", encoding="utf-8") as f:
            f.write(f"{now},{broker.state()[0]:.2f},{'reel' if broker.live else 'simulation'}\n")
        days = getattr(broker, "days_left", None)
        if days is not None and days < KEY_WARN_DAYS and time.gmtime(now).tm_hour == 0:     # une fois par jour : sans cle valide, plus d'entree NI de sortie
            alerts.append(f"LA CLE D'AGENT EXPIRE DANS {days:.0f} JOURS : a renouveler (secrets HL_AGENT_KEY)")
    write("announcements.csv", A_COLS, [dict(zip(A_COLS, r)) for r in merged])
    write("journal.csv", J_COLS, journal)
    (LIVE / "alerts.txt").write_text("\n".join(alerts), encoding="utf-8")
    print(f"{time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(now))} | annonces connues {len(merged)} (+{len(fresh)}) | journal {len(journal)} lignes | "
          f"ouverts {sum(j['status'] == 'ouvert' for j in journal)} | signaux en attente {sum(j['status'] == 'signal' for j in journal)} | "
          f"perps Aster {len(aster_syms)} | courtier {'absent' if not broker else 'REEL' if broker.live else 'simulation'}")
    print("\n".join(alerts))


def _selftest():
    global book_snapshot, candles, funding_sum
    assert schedule(1000000000) == (1000000000 + DELAY_S + 200 + 300, 1000000000 + DELAY_S + 500 + HOLD_S)         # 1e9 + 36 h n'est pas sur une bougie : arrondi au-dessus
    j0 = {c: "" for c in J_COLS} | {"id": "x", "ticker": "Z", "exch": "hl", "coin": "Z", "status": "signal", "entry_ts": 1000, "exit_ts": 1000 + HOLD_S, "t_ann": 0}
    assert j0["exit_ts"] - j0["entry_ts"] == HOLD_S
    assert schedule(1000000000)[0] % 300 == 0 and schedule(999999900) == (999999900 + DELAY_S + 300, 999999900 + DELAY_S + 300 + HOLD_S)   # deja sur une bougie : pas d'arrondi
    uni = {"SUI": {}, "kPEPE": {}, "BTC": {}}
    assert hl_coin("SUI", uni) == "SUI" and hl_coin("PEPE", uni) == "kPEPE" and hl_coin("ZAMA", uni) is None
    assert aster_bases(["1000PEPEUSDT", "ZAMAUSDT", "BTCUSD1"]) == {"PEPE": "1000PEPEUSDT", "ZAMA": "ZAMAUSDT"}
    t = 2000000000
    good = [{"t": (t - 3 * 86400 + 300 * i) * 1000, "c": str(1 + i % 7)} for i in range(864)]
    assert eligible(good, t) == (True, "")
    assert eligible(good[:500], t)[0] is False and eligible([c | {"c": "1"} for c in good], t) == (False, "perp fige (aucune variation en 24 h)")
    bids = [{"px": "100", "sz": "5"}, {"px": "99", "sz": "10"}]                                  # 500 $ a 100 puis 990 $ a 99
    assert exec_price(bids, 500) == 100 and abs(exec_price(bids, 995) - 995 / (5 + 5)) < 1e-9 and exec_price(bids, 5000) is None
    s, h, tot = paper_pnl(100, 90, 0.002, [10, 20], [11, 20], 0.001)
    assert abs(s - (0.10 - TAKER["hl"] * 1.9 + 0.002)) < 1e-12 and abs(h - (0.05 - 0.001 - 2 * TAKER["hl"])) < 1e-12 and abs(tot - s - h) < 1e-12
    assert sprt([0.05] * 10)[2] == "en cours" and sprt([0.05] * 70)[2] == "VALIDE" and sprt([-0.03] * 40)[2] == "ARRET"
    journal, alerts = [], []                                                                      # machine a etats, sans reseau
    step(t, journal, [{"ts": t - 3600, "venue": "upbit", "ticker": "ZAMA"}], alerts, uni, {"XYZ": "XYZUSDT"})
    assert journal[0]["status"] == "ineligible" and "Aster" in journal[0]["note"]
    step(t, journal, [{"ts": t - 45 * 3600, "venue": "upbit", "ticker": "SUI"}], alerts, uni)     # vue 45 h apres : au-dela de +36 h + LATE_S
    assert journal[1]["note"] == "annonce vue trop tard" and len(alerts) == 2

    class Fake:                                                                                   # courtier sans reseau : tient ses positions, enregistre les ordres
        def __init__(self, live):
            self.live, self.sent, self.pos, self.fail = live, [], {}, set()
        def state(self): return 150.0, dict(self.pos)
        def size_for(self, coin, usd, px): return round(usd / px, 4)
        def leverage(self, coin): pass
        def market(self, coin, side, size, ref, reduce=False):
            if (coin, side) in self.fail:
                raise RuntimeError("refus simule")
            self.sent.append((coin, side, size, reduce))
            self.pos[coin] = round(self.pos.get(coin, 0.0) + (size if side == "buy" else -size), 10)
            self.pos = {c: z for c, z in self.pos.items() if z}
            return size, ref
        def stop(self, coin, size, trigger):
            if ("stop", coin) in self.fail:
                raise RuntimeError("stop refuse")
            return f"stop@{trigger:.2f}"
        def cancel_all(self, coin): pass
    ctx2 = {"BTC": {"midPx": "90000", "dayNtlVlm": "9e9"}, "ETH": {"midPx": "2000", "dayNtlVlm": "5e9"}, "SOL": {"midPx": "100", "dayNtlVlm": "4e9"},
            "HYPE": {"midPx": "50", "dayNtlVlm": "3e9"}, "SUI": {"midPx": "1", "dayNtlVlm": "1e8", "funding": "0"}, "XRP": {"midPx": "2", "dayNtlVlm": "2e9"}}
    book_snapshot = lambda exch, coin: {"bid": 1.0, "ask": 1.001, "mid": 1.0005, "spread_bp": 10.0, "sell": [1.0, 0.999], "buy": [1.001, 1.002]}
    candles = lambda exch, coin, a, b: [{"t": a * 1000, "T": b * 1000, "o": "1", "h": "1.01", "c": "1"}]
    funding_sum = lambda exch, coin, a, b: 0.0
    b = Fake(live=True)
    sig = {c: "" for c in J_COLS} | {"id": "x", "ticker": "SUI", "exch": "hl", "coin": "SUI", "status": "signal", "entry_ts": t, "exit_ts": t + HOLD_S, "t_ann": t - DELAY_S}
    jr, al = [sig], []
    step(t + 60, jr, [], al, ctx2, {}, b)
    assert sig["status"] == "ouvert" and sig["r_size"] == 22.0 and sig["r_net_usd"] == "" and b.pos["SUI"] == -22.0, (sig["status"], al)     # REGRESSION : ouverte a ce passage, pas refermee aussitot
    assert [c for c, _, _ in json.loads(sig["r_legs"])] == ["ETH", "SOL"] and "SUI" not in json.loads(sig["legs"]) and expected(jr) == b.pos and not [x for x in al if "ECART" in x]
    step(t + 3660, jr, [], al, ctx2, {}, b)
    assert sig["status"] == "ouvert"                                                              # passage suivant : la position est sur le compte, rien ne bouge
    b.fail = {("SOL", "sell")}                                                                    # sortie : la 2e ligne refuse de se vendre
    step(t + HOLD_S + 60, jr, [], al, ctx2, {}, b)
    n_buy = sum(x[:2] == ("SUI", "buy") for x in b.sent)
    assert sig["status"] == "clos" and sig["r_px_out"] != "" and sig["r_net_usd"] == "" and n_buy == 1 and b.pos == {"SOL": json.loads(sig["r_legs"])[1][1]}
    b.fail = set()
    step(t + HOLD_S + 3660, jr, [], al, ctx2, {}, b)                                              # nouvel essai : ne rachete PAS le short une 2e fois, ne revend PAS ETH
    assert sig["r_net_usd"] != "" and sum(x[:2] == ("SUI", "buy") for x in b.sent) == 1 and sum(x[:2] == ("ETH", "sell") for x in b.sent) == 1 and b.pos == {} and expected(jr) == {}
    b2 = Fake(live=True)                                                                          # stop refuse a l'entree, repose au passage suivant ; puis rachete par l'exchange
    b2.fail = {("stop", "SUI")}
    s2 = dict(sig) | {c: "" for c in R_COLS} | {"status": "signal", "t_in": "", "net_hedged": ""}
    j2, a2 = [s2], []
    step(t + 60, j2, [], a2, ctx2, {}, b2)
    assert s2["r_stop"] == "" and any("STOP NON POSE" in x for x in a2)
    b2.fail = set()
    step(t + 3660, j2, [], a2, ctx2, {}, b2)
    assert s2["r_stop"].startswith("stop@") and any("STOP REPOSE" in x for x in a2)
    del b2.pos["SUI"]                                                                             # le stop de l'exchange a rachete le short
    step(t + 7260, j2, [], a2, ctx2, {}, b2)
    assert s2["status"] == "stoppe" and "stop de l'exchange" in s2["r_note"] and sum(x[:2] == ("SUI", "buy") for x in b2.sent) == 0 and b2.pos == {} and s2["r_net_usd"] != ""
    eth = {c: "" for c in J_COLS} | {"id": "e", "ticker": "ETH", "exch": "hl", "coin": "ETH", "status": "ouvert", "legs": "[]", "r_note": ""}
    real_enter(eth, {"bid": 2000.0}, ctx2, Fake(live=False), [dict(sig) | {"r_net_usd": "", "r_px_out": "", "r_legs": json.dumps([["ETH", 0.0055, 2000]]), "r_legs_out": ""}, eth], [])
    assert eth["r_size"] == "" and "deja en portefeuille" in eth["r_note"]                        # ETH sert de couverture ailleurs : pas de short reel dessus
    print("self-check OK")


if __name__ == "__main__":
    _selftest() if "--test" in sys.argv else main()
