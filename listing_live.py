"""Suivi EN DIRECT de la regle listing v1 : annonces -> signaux -> journal, avec le carnet releve a l'entree et a la sortie.

    python listing_live.py          # un passage : a lancer toutes les heures (.github/workflows/listing-live.yml du depot de deploiement)
    python listing_live.py --test   # self-check, sans reseau

Sans cle : journal FICTIF seul (API publiques, requests + listing_feed.py). Avec des cles, le meme journal porte aussi les ordres du pilote : colonnes
r_*, sur Hyperliquid (listing_broker.py, HL_LIVE=1) et sur Aster (listing_broker_aster.py, ASTER_LIVE=1) ; sans la variable, simulation. Exchange d'un
signal : Hyperliquid si le perp y existe (le moins cher), sinon Aster (research/execution-venues.md). Chaque exchange est un compte autonome : capital,
marge, couverture (memes coins, sur le meme exchange), plafonds et rapprochement propres. Un signal dont le perp existe sur les deux est pris sur les
deux (ligne jumelle) : cible F x w x somme des capitaux, repartie pour egaliser l'utilisation des comptes (allocate). Etat dans live/ (CSV versionnes) :
  announcements.csv  annonces retenues (memes analyseurs et meme dedoublonnage que le backtest)
  journal.csv        un signal par ligne : signal -> ouvert -> clos | stoppe, ou ineligible / entree manquee
  alerts.txt         ce qui s'est passe pendant CE passage (la tache en fait une issue)
  health.json        nombre de passages rates de suite, par source
  markets.json       tickers en won cotes sur Upbit et Bithumb au dernier passage (detecteur de listing de secours)
  equity.csv         valeur du compte Hyperliquid a chaque passage (equity_aster.csv : compte Aster)
  STOP               s'il existe, plus aucune entree reelle (les sorties continuent) : arret d'urgence, a creer a la main dans le depot, ou cree par un
                     arret dur (hard_stops) ; on le supprime a la main pour reprendre
Regle : research/listing-strategy-v1.md (v1.1). Short a +36 h, 5 jours, stop +50 %, couverture = panier equipondere fixe HEDGE (hors coins shortes
par ailleurs), beta 1. Pilote reel : short de F x capital x min(1, VOL_REF / vol 3 j), couverture au meme notionnel, short total <= MAX_SHORT x capital.
"""
import csv
import json
import math
import pathlib
import re
import statistics
import sys
import time

import requests

import listing_feed as lf

LIVE = pathlib.Path("live")
INFO = "https://api.hyperliquid.xyz/info"
ASTER = "https://fapi.asterdex.com"
VENUES = {("upbit", "spot_krw"), ("bithumb", "spot_krw"), ("coinbase", "spot"), ("binance", "spot"), ("robinhood", "spot"),
          ("binance", "perp")}                                  # listing de PERP Binance : suivi FICTIF seulement (spec 18), jamais d'ordre reel
DELAY_S, HOLD_S, STOP = 36 * 3600, 120 * 3600, 0.5
LATE_S = 6 * 3600                  # tolerance de retard a l'entree. La tache GitHub ne delivre que ~1 passage sur 4 (trous jusqu'a 4,6 h) ; le backtest montre
                                   # un plateau du delai d'entree (+36 h a +60 h : +259 a +328 pb couvert), donc entrer tard vaut bien mieux que manquer le trade.
LOOKBACK_S = 12 * 3600             # profondeur de relecture des fils a chaque passage (la tache GitHub peut sauter des heures)
HEDGE = ("ETH", "SOL", "DOGE", "BNB")  # v1.1 (2026-10-06) : meme risque retire que le panier de mid-caps teste (~25 %), mais celui-ci chutait de 252 pb
                                    # pendant les tenues et annulait une partie de l'edge ; XRP retire (le moins correle), BTC moins bon a tout ratio
BEST_EFFORT = {"upbit", "bithumb"}  # sources redondantes (les fils couvrent Upbit a 96 % et Bithumb a 97 %) : une panne ne merite pas d'alerte
ALERT_AFTER = (6, 24, 72)          # nombre de passages rates de suite qui declenchent une alerte sur une source
SIZES = (1000, 5000)               # notionnels ($) pour lesquels on releve le prix executable
TAKER = {"hl": 0.00045, "aster": 0.00035}
F, VOL_REF, MAX_REAL = 0.5, 0.08, 4            # pilote reel : short de 50 % du capital, reduit si la vol journaliere des 3 j avant l'annonce depasse 8 % (backtest)
LEV_SHORT, LEV_HEDGE = 3, 10                   # marge croisee seulement, jamais isolee sur un short (framework, section 4) ; plafonnes par l'exchange
MAX_MARGIN, MIN_LEG = 0.7, {"hl": 11, "aster": 6}   # marge initiale du compte <= 70 % de sa valeur ; ordre minimum : Hyperliquid 10 $, Aster 5 $
MAX_SHORT = 1.0                                # short total <= 1 x le capital : au-dela de ~1,25 x, un squeeze commun de +50 % liquiderait le compte AVANT les stops
SPRT_DRIFT, SPRT_BOUND, SPRT_NMAX = 0.00828, 1.337, 264    # Wald H0 0 / H1 238 pb sur le resultat couvert PONDERE par la taille (w = min(1, VOL_REF / vol)),
                                               # sigma pondere 1 004 pb, erreurs 10 % / 10 % ; simule : arret a tort 12 % si 238 pb, validation a tort 11 % si 0, ~108 trades
BALANCE_ALERT = 0.6                            # alerte quotidienne si un compte porte plus de 60 % du capital total : transfert manuel (les cles ne retirent rien)
V11_FROM = 1791277200                          # 2026-10-06 09:00 UTC : la v1.1 (couverture, test) repart de zero (research/listing-strategy-v1.md)
STOPS_MAX, FUND_MIN, FLOOR_USD = (5, 30), -0.02, 100.0     # arrets durs : >= 5 stops sur les 30 derniers trades, funding moyen < -200 pb sur 20, compte < 100 $
MAX_NEW_MARKETS = 8                # plus de 8 marches neufs d'un coup = etat perdu, pas 8 listings : on reamorce sans emettre
KEY_WARN_DAYS = 14
A_COLS = ["ts", "venue", "market", "ticker", "n_tickers", "head", "source"]
R_COLS = ["real", "r_size", "r_px_in", "r_stop", "r_legs", "r_px_out", "r_legs_out", "r_net_usd", "r_note", "r_out"]     # r_out : [quantite rachetee, prix moyen] du short
J_COLS = ["id", "t_ann", "venue", "ticker", "exch", "coin", "status", "note", "entry_ts", "exit_ts", "t_in", "bid", "ask", "spread_bp", "px_in", "slip_1k_bp", "slip_5k_bp",
          "funding_in", "day_vol_musd", "legs", "legs_px_in", "t_out", "px_out", "funding_sum", "legs_px_out", "legs_funding", "short_net", "hedge_net", "net_hedged"] + R_COLS + [
          "market", "vol3d", "twin"]                   # vol3d : vol journaliere des 3 j avant l'annonce ; twin : id de la ligne principale d'une ligne jumelle


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


def vol3d(candles, t):
    """Bougies 5 m -> volatilite journaliere des 3 jours avant t (ecart-type de ln(c/o) x racine de 288, comme le backtest), ou None."""
    rets = [math.log(float(c["c"]) / float(c["o"])) for c in candles if t - 3 * 86400 <= c["t"] // 1000 < t]
    return statistics.stdev(rets) * 288 ** 0.5 if len(rets) > 1 else None


def weight(j):
    """Poids de taille du trade, celui du pilote reel : min(1, VOL_REF / vol 3 j) ; 1 si la vol n'a pas ete relevee."""
    return min(1.0, VOL_REF / float(j["vol3d"])) if j["vol3d"] not in ("", "0", "0.0") else 1.0


def sprt(values):
    """Resultats couverts ponderes (w x net) dans l'ordre des sorties -> (n, S, verdict). Valider si S >= d n + b, arreter si S <= d n - b. Le premier
    franchissement est definitif ; a SPRT_NMAX trades sans franchissement, le signe de S - d n tranche."""
    s = 0.0
    for n, v in enumerate(values, 1):
        s += v
        if s >= SPRT_DRIFT * n + SPRT_BOUND:
            return n, s, "VALIDE"
        if s <= SPRT_DRIFT * n - SPRT_BOUND:
            return n, s, "ARRET"
        if n == SPRT_NMAX:
            return n, s, "VALIDE" if s >= SPRT_DRIFT * n else "ARRET"
    return len(values), s, "en cours"


def v1_closed(journal, since=0):
    """Trades fictifs clos de la regle v1 (hors suivi des listings de perp, autre regle) entres depuis `since`, dans l'ordre des sorties."""
    return sorted((x for x in journal if x["net_hedged"] != "" and x.get("market") != "perp" and not x.get("twin") and int(x["t_in"] or 0) >= since),
                  key=lambda x: int(x["t_out"]))                    # une ligne jumelle (meme signal, autre exchange) n'est jamais comptee deux fois


def sprt_v11(journal):
    return sprt([weight(x) * float(x["net_hedged"]) for x in v1_closed(journal, V11_FROM)])


def hard_stops(journal, values=None):
    """Arrets durs (research/listing-strategy-v1.md, recalibres le 2026-10-06) -> raisons ; chacune cree live/STOP. values = {exchange: valeur du compte reel}."""
    v1, why = v1_closed(journal), []
    n, s, verdict = sprt_v11(journal)
    if verdict == "ARRET":
        why.append(f"test sequentiel a l'arret (n {n}, S {s:+.3f})")
    last = v1[-STOPS_MAX[1]:]
    if sum(x["status"] == "stoppe" for x in last) >= STOPS_MAX[0]:
        why.append(f"{sum(x['status'] == 'stoppe' for x in last)} stops sur les {len(last)} derniers trades")
    fund = [float(x["funding_sum"]) for x in v1[-20:] if x["funding_sum"] != ""]
    if len(fund) == 20 and sum(fund) / 20 < FUND_MIN:
        why.append(f"funding moyen du short {sum(fund) / 20 * 1e4:+.0f} pb sur les 20 derniers trades")
    for ex, v in (values or {}).items():
        if v is not None and v < FLOOR_USD:
            why.append(f"compte {ex} a {v:.2f} $, sous le plancher de {FLOOR_USD:.0f} $")
    return why


def allocate(target, accounts):
    """Repartition d'un trade entre comptes. accounts = {exchange: (valeur E, short ouvert S, marge bloquee M, ordre minimum)} -> {exchange: montant en $}.
    Egalise l'utilisation S / E (le compte le moins charge recoit le plus), sous les plafonds de chaque compte (short <= MAX_SHORT x E, marge <= MAX_MARGIN x E) ;
    si la place manque, le trade est reduit ; une part non nulle sous l'ordre minimum est abandonnee et le reste reparti a nouveau."""
    acc = dict(accounts)
    while acc:
        cap = {v: max(0.0, min(MAX_SHORT * E - S, (MAX_MARGIN * E - M) / (1 / LEV_SHORT + 1 / LEV_HEDGE))) for v, (E, S, M, _) in acc.items()}
        goal = min(target, sum(cap.values()))
        fill = lambda lam: {v: min(cap[v], max(0.0, lam * E - S)) for v, (E, S, _, _) in acc.items()}
        lo, hi = 0.0, max((S + cap[v]) / E for v, (E, S, _, _) in acc.items()) + 1.0
        for _ in range(100):                                        # niveau d'utilisation commun : la somme servie croit avec lui
            mid = (lo + hi) / 2
            lo, hi = (mid, hi) if sum(fill(mid).values()) < goal else (lo, mid)
        x = fill(hi)
        small = [v for v in acc if 0 < x[v] < acc[v][3]]
        if not small:
            return {v: round(z, 6) for v, z in x.items() if z >= acc[v][3]}
        acc.pop(min(small, key=x.get))
    return {}


def done_qty(x, size):
    """Element de r_legs_out [coin, prix] (ancien format : vendu en entier) ou [coin, prix moyen, quantite] -> quantite deja vendue."""
    return float(x[2]) if len(x) > 2 else size


def expected(journal, mode="reel", exch="hl"):
    """Positions que le compte `exch` DOIT porter d'apres le journal, pour les lignes du mode donne : {coin ou symbole: taille signee}. Short tant
    qu'il n'est pas entierement rachete, lignes tant qu'elles ne sont pas entierement vendues."""
    want = {}
    for j in journal:
        if j["r_size"] == "" or j["r_net_usd"] != "" or j["real"] != mode or j["exch"] != exch:
            continue
        if j["r_px_out"] == "":
            want[j["coin"]] = want.get(j["coin"], 0.0) - float(j["r_size"]) + json.loads(j["r_out"] or "[0, 0]")[0]
        sold = {x[0]: x for x in json.loads(j["r_legs_out"] or "[]")}
        for c, z, _ in json.loads(j["r_legs"] or "[]"):
            left = z - (done_qty(sold[c], z) if c in sold else 0.0)
            if left > 1e-12:
                want[c] = want.get(c, 0.0) + left
    return {c: z for c, z in want.items() if abs(z) > 1e-12}


def price(ctx, c, default):
    """Prix de reference d'un perp Hyperliquid, ou `default` s'il n'est plus dans l'univers (radie)."""
    x = ctx.get(c) or {}
    return float(x.get("midPx") or x.get("markPx") or default)


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
    """Perps Aster negociables ET crypto : actions, ETF, matieres premieres (METAUSDT = l'action Meta, PAXGUSDT...) exclus (listing_feed.aster_crypto)."""
    return [s["symbol"] for s in aster("/fapi/v1/exchangeInfo")["symbols"] if s.get("status") == "TRADING" and lf.aster_crypto(s)]


def aster_prices():
    """Dernier prix de chaque perp Aster -> {symbole: {"midPx": prix}} : meme forme que le contexte Hyperliquid, pour price()."""
    return {x["symbol"]: {"midPx": x["price"]} for x in aster("/fapi/v1/ticker/price")}


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
            tp = lf.parse_binance_perp(a["title"])
            if tp:
                out += [(a["releaseDate"] // 1000, "binance", "perp", t, len(tp), a["title"][:200], "binance_cms") for t in tp]
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

def real_enter(j, snap, ctx, broker, journal, alerts, cands=None, target=None):
    """Short dimensionne sur le capital du compte de l'exchange, stop de protection sur l'exchange, puis la couverture au meme notionnel, sur le meme
    exchange. ctx = prix de cet exchange ; cands = lignes de couverture candidates (noms de cet exchange) ; target = part de ce compte fixee par allocate(),
    sinon F x capital x w. Les plafonds du compte restent verifies ici. Une erreur n'interrompt jamais le journal fictif."""
    mode, coin, ex = "reel" if broker.live else "simulation", j["coin"], j["exch"]
    if (LIVE / "STOP").exists():
        j["r_note"] = "arret d'urgence : pas d'entree"
        return
    if sum(x["r_size"] != "" and x["r_net_usd"] == "" and x["exch"] == ex for x in journal if x is not j) >= MAX_REAL:
        j["r_note"] = f"deja {MAX_REAL} positions : pas d'entree"
        return
    if coin in expected(journal, mode, ex) or (broker.live and coin in broker.state()[1]):
        j["r_note"] = "ce perp est deja en portefeuille (position ou ligne de couverture) : pas d'entree"      # Hyperliquid compense les positions d'un meme coin
        alerts.append(f"ENTREE {mode.upper()} ECARTEE {coin} : perp deja en portefeuille")
        return
    try:
        value, have, used = broker.state()
        if j["vol3d"] == "":
            t = int(j["t_ann"])
            j["vol3d"] = round(vol3d(candles(ex, coin, t - 3 * 86400, t), t) or VOL_REF, 5)
        vol = float(j["vol3d"])
        short_open = sum(-z * price(ctx, c, 0.0) for c, z in have.items() if z < 0)
        caps = {"taille cible": F * value * min(1.0, VOL_REF / vol) if target is None else target, "marge": (MAX_MARGIN * value - used) / (1 / LEV_SHORT + 1 / LEV_HEDGE),
                "plafond d'exposition": MAX_SHORT * value - short_open}
        usd = min(caps.values())
        j["r_note"] = (("" if target is None else f"part de la repartition {target:.0f} $ ; ") +
                       f"taille {usd:.0f} $ (compte {value:.0f} $, w = min(1, {VOL_REF:.0%} / vol {vol:.1%})), marge deja bloquee {used:.0f} $, "
                       f"short deja ouvert {short_open:.0f} $ (limite : {min(caps, key=caps.get)})")
        if usd < MIN_LEG[ex]:
            j["r_note"] += " : marge ou plafond d'exposition atteint, pas d'entree"
            alerts.append(f"ENTREE {mode.upper()} ECARTEE {coin} : {j['r_note']}")
            return
        broker.leverage(coin, LEV_SHORT)
        size, px = broker.market(coin, "sell", broker.size_for(coin, usd, snap["bid"]), snap["bid"])
    except Exception as e:
        j["r_note"] = f"entree en echec : {e}"
        alerts.append(f"ENTREE {mode.upper()} EN ECHEC {coin} : {e}")
        return
    j |= {"real": mode, "r_size": size, "r_px_in": px}
    try:
        j["r_stop"] = broker.stop(coin, size, px * (1 + STOP))
    except Exception as e:
        alerts.append(f"URGENT : STOP NON POSE sur {coin} ({e}) -> nouvelle tentative au prochain passage")
    cands = json.loads(j["legs"]) if cands is None else cands       # deja hors coins shortes
    k = max(1, min(len(cands), int(size * px // MIN_LEG[ex])))      # autant de lignes que le minimum d'ordre le permet
    legs, left = [], size * px
    for i, c in enumerate(cands):
        if len(legs) == k:
            break
        ref = price(ctx, c, 0.0)
        target = left / min(k - len(legs), len(cands) - i)          # une ligne sautee reporte son notionnel sur les suivantes
        try:
            if ref <= 0:
                raise RuntimeError("prix inconnu")
            want = broker.size_for(c, target, ref)
            if abs(want * ref / target - 1) > 0.15:                 # pas de cotation trop gros pour la cible (ZEC : 0,01 = 15 $) : ligne suivante
                continue
            broker.leverage(c, LEV_HEDGE)
            lsize, lpx = broker.market(c, "buy", want, ref)
            legs.append([c, lsize, lpx])
            left -= lsize * lpx
        except Exception as e:
            alerts.append(f"COUVERTURE INCOMPLETE {c} : {e}")
    j["r_legs"] = json.dumps(legs)
    alerts.append(f"ENTREE {mode.upper()} short {size} {coin} a {px} ({size * px:.0f} $) ; stop {j['r_stop'] or 'ABSENT'} ; "
                  f"couverture {size * px - left:.0f} $ {[(c, z) for c, z, _ in legs]} ; {j['r_note']}")


def real_exit(j, ctx, broker, positions, alerts):
    """Rachat du short puis vente des lignes. Chaque quantite executee est ecrite aussitot : une execution partielle (IOC plafonne a 1 %) laisse le reste
    au passage suivant, sans jamais racheter ni vendre deux fois. Le stop de l'exchange n'est annule qu'une fois le short entierement rachete."""
    coin, size, px_in = j["coin"], float(j["r_size"]), float(j["r_px_in"])
    if (j["real"] == "reel") != broker.live:                       # HL_LIVE a change avec des positions ouvertes : ni sortie simulee d'une position reelle,
        alerts.append(f"SORTIE {j['real'].upper()} {coin} EN ATTENTE : courtier en mode {'reel' if broker.live else 'simulation'}")    # ni ordre reel pour une simulee
        return
    try:
        if j["r_px_out"] == "":
            done = json.loads(j["r_out"] or "[0, 0]")
            stop_px = px_in * (1 + STOP) * 1.005
            complete = done[0] >= size - 1e-9                       # tout rachete a un passage precedent (puis echec avant l'ecriture du prix)
            if not complete and broker.live and coin not in positions:      # le stop de l'exchange a rachete (le reste)
                j |= {"r_px_out": (done[0] * done[1] + (size - done[0]) * stop_px) / size, "r_note": "rachete par le stop de l'exchange (prix estime)"}
            else:
                left = 0.0 if complete else -positions.get(coin, 0.0) if broker.live else size - done[0]
                if left > 1e-12:
                    filled, px = broker.market(coin, "buy", round(left, 10), book_snapshot(j["exch"], coin)["ask"], reduce=True)
                    done = [done[0] + filled, (done[0] * done[1] + filled * px) / (done[0] + filled)]
                    j["r_out"] = json.dumps(done)
                    if filled < left - 1e-9:
                        raise RuntimeError(f"rachat partiel ({done[0]:g} sur {size:g}) : stop maintenu, suite au prochain passage")
                broker.cancel_all(coin)
                j["r_px_out"] = done[1]
        sold = {x[0]: x for x in json.loads(j["r_legs_out"] or "[]")}
        for c, lsize, lpx in json.loads(j["r_legs"] or "[]"):
            q0 = done_qty(sold[c], lsize) if c in sold else 0.0
            if lsize - q0 <= 1e-12:
                continue
            if broker.live and c not in positions:                 # plus aucune position sur ce coin (radie, liquide) : rien a vendre, prix estime
                sold[c] = [c, price(ctx, c, lpx), lsize]
            else:
                filled, px = broker.market(c, "sell", round(lsize - q0, 10), price(ctx, c, lpx), reduce=True)
                sold[c] = [c, ((sold[c][1] * q0 if c in sold else 0.0) + px * filled) / (q0 + filled), q0 + filled]
            j["r_legs_out"] = json.dumps(list(sold.values()))
            if done_qty(sold[c], lsize) < lsize - 1e-9:
                raise RuntimeError(f"vente partielle de {c} ({done_qty(sold[c], lsize):g} sur {lsize:g}), suite au prochain passage")
        px_out, out, t_in, t_out = float(j["r_px_out"]), {c: float(x[1]) for c, x in sold.items()}, int(j["t_in"]), int(j["t_out"])
        fee, ex = TAKER[j["exch"]], j["exch"]
        usd = size * (px_in - px_out) - fee * size * (px_in + px_out) + size * px_in * funding_sum(ex, coin, t_in, t_out)
        for c, lsize, lpx in json.loads(j["r_legs"] or "[]"):
            usd += lsize * (out[c] - lpx) - fee * lsize * (out[c] + lpx) - lsize * lpx * funding_sum(ex, c, t_in, t_out)
    except Exception as e:
        alerts.append(f"SORTIE {j['real'].upper()} INCOMPLETE {coin} : {e} -> suite au prochain passage")
        return
    j["r_net_usd"] = round(usd, 4)
    alerts.append(f"SORTIE {j['real'].upper()} {coin} : {usd:+.2f} $ ({usd / (size * px_in) * 1e4:+.0f} pb du notionnel)")


# ---------------------------------------------------------------- machine a etats

def step(now, journal, new_ann, alerts, ctx, aster_syms=None, broker=None, actx=None):
    """Fait avancer le journal. ctx = {perp Hyperliquid: contexte de marche} ; aster_syms = {ticker: symbole Aster} ; broker = {exchange: courtier} (un
    courtier seul vaut pour Hyperliquid), ou None ; actx = {symbole Aster: {"midPx": prix}}."""
    brokers = dict(broker) if isinstance(broker, dict) else {"hl": broker} if broker else {}
    pctx = {"hl": ctx, "aster": actx or {}}
    positions = {}                                                  # etat des comptes AVANT ce passage : ne sert qu'aux positions ouvertes a un passage precedent
    for ex, b in list(brokers.items()):
        if b.live:
            try:
                positions[ex] = b.state()[1]
            except Exception as e:                                  # un compte illisible n'empeche ni le fictif ni l'autre exchange
                alerts.append(f"COMPTE {ex.upper()} ILLISIBLE ({type(e).__name__}) : aucun ordre sur cet exchange a ce passage")
                del brokers[ex]
    fam = lambda x: x.get("market") == "perp"                       # deux familles independantes : un suivi fictif de perp ne bloque jamais un signal spot reel
    busy = {(fam(j), j["ticker"]) for j in journal if j["status"] in ("signal", "ouvert")}
    for a in new_ann:                                               # 1. nouvelles annonces -> signal ou ineligible
        t, tick, perp = int(a["ts"]), a["ticker"].upper(), a.get("market") == "perp"
        coin = hl_coin(tick, ctx)
        exch, coin = ("hl", coin) if coin else ("aster", (aster_syms or {}).get(tick))
        entry, exit_ = schedule(t)
        j = {c: "" for c in J_COLS} | {"id": f"{a['venue']}-{'perp-' if perp else ''}{tick}-{t}", "t_ann": t, "venue": a["venue"], "ticker": tick, "exch": exch if coin else "",
                                       "coin": coin or "", "entry_ts": entry, "exit_ts": exit_, "market": a.get("market", "")}
        recent = any(fam(x) == perp and x["ticker"] == tick and x["status"] != "ineligible" and abs(int(x["t_ann"]) - t) < 86400 for x in journal)
        if coin is None:
            j |= {"status": "ineligible", "note": "pas de perp sur Hyperliquid ni sur Aster" if aster_syms else "pas de perp sur Hyperliquid (Aster injoignable)"}
        elif (perp, tick) in busy or recent:
            j |= {"status": "ineligible", "note": "deja un signal ou une position sur ce coin"}
        elif now > entry + LATE_S:
            j |= {"status": "ineligible", "note": "annonce vue trop tard"}
        else:
            try:
                cs = candles(exch, coin, t - 3 * 86400, t)
                ok, why = eligible(cs, t)
                j["vol3d"] = round(vol3d(cs, t) or 0.0, 5) or ""
            except Exception as e:                                  # reseau : le signal est garde, l'eligibilite sera verifiee a l'entree
                ok, why = True, f"eligibilite a verifier a l'entree ({type(e).__name__})"
            j |= {"status": "signal" if ok else "ineligible", "note": why}
            j["note"] = why or ("listing de perp : suivi fictif seulement (spec 18)" if perp else "")
            busy |= {(perp, tick)} if ok else set()
        journal.append(j)
        a_c, also = (aster_syms or {}).get(tick), ""
        if j["status"] == "signal" and j["exch"] == "hl" and a_c and not perp:    # perp aussi sur Aster : ligne jumelle, le signal est pris sur les deux comptes
            tw = j | {"id": j["id"] + "-aster", "exch": "aster", "coin": a_c, "twin": j["id"], "vol3d": ""}
            try:
                cs = candles("aster", a_c, t - 3 * 86400, t)
                ok, why = eligible(cs, t)
                tw["vol3d"] = round(vol3d(cs, t) or 0.0, 5) or ""
            except Exception as e:
                ok, why = True, f"eligibilite a verifier a l'entree ({type(e).__name__})"
            tw |= {"status": "signal" if ok else "ineligible", "note": why}
            journal.append(tw)
            also = f" et aster:{a_c}" if ok else f" (Aster ecarte : {why})"
        alerts.append(f"ANNONCE {a['venue']} {tick} -> {j['status']} {j['note']}" + (f" | entree prevue {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(entry))} sur {exch}:{coin}{also}" if j["status"] == "signal" else ""))
    def paper_enter(j):
        """Entree fictive d'une ligne -> (carnet, lignes de couverture), ou None si elle est reportee ou ecartee. Une ligne en erreur ne bloque pas les autres."""
        try:
            if j["note"].startswith("eligibilite a verifier"):
                t = int(j["t_ann"])
                cs = candles(j["exch"], j["coin"], t - 3 * 86400, t)
                ok, why = eligible(cs, t)
                j |= {"note": why, "vol3d": round(vol3d(cs, t) or 0.0, 5) or ""}
                if not ok:
                    j["status"] = "ineligible"
                    alerts.append(f"SIGNAL ECARTE A L'ENTREE {j['coin']} : {why}")
                    return None
            s = book_snapshot(j["exch"], j["coin"])
            if j["exch"] == "aster":                                # identite : le perp Aster est-il le coin liste ? (6 collisions de tickers connues)
                ok, ratio = lf.same_asset(lf.ref_price(j["venue"], j["market"], j["ticker"]), s["mid"], lf.lot(j["coin"]) / lf.lot(j["ticker"]))
                if ratio is None:
                    alerts.append(f"ENTREE REPORTEE {j['coin']} : identite non verifiable (pas encore de prix sur {j['venue']}) -> nouvel essai au prochain passage")
                    return None
                if not ok:
                    j |= {"status": "ineligible", "note": f"identite douteuse : prix Aster = {ratio:.2f} x prix sur {j['venue']}"}
                    alerts.append(f"SIGNAL ECARTE A L'ENTREE {j['coin']} : {j['note']}")
                    return None
            shorted = {x["ticker"] for x in journal if x["status"] in ("signal", "ouvert")} | {j["ticker"]}
            legs = [c for c in HEDGE if c in ctx and c not in shorted]
            px = s["sell"][0] or s["bid"]
            slip = [None if p is None else (s["mid"] - p) / s["mid"] * 1e4 for p in s["sell"]]
            vol = float(ctx[j["coin"]].get("dayNtlVlm") or 0) / 1e6 if j["exch"] == "hl" else ""
            # exit_ts est recalcule ici : la tenue de 5 jours part de l'entree REELLE, comme dans le backtest, meme si le passage horaire est en retard
            j |= {"status": "ouvert", "t_in": now, "exit_ts": now + HOLD_S, "bid": s["bid"], "ask": s["ask"], "spread_bp": round(s["spread_bp"], 2), "px_in": px,
                  "slip_1k_bp": "" if slip[0] is None else round(slip[0], 2), "slip_5k_bp": "" if slip[1] is None else round(slip[1], 2),
                  "funding_in": ctx[j["coin"]].get("funding", "") if j["exch"] == "hl" else "", "day_vol_musd": "" if vol == "" else round(vol, 2),
                  "legs": json.dumps(legs), "legs_px_in": json.dumps([price(ctx, c, 0.0) for c in legs])}
        except Exception as e:
            alerts.append(f"ENTREE REPORTEE {j['exch']}:{j['coin']} ({type(e).__name__} : {e}) -> nouvel essai au prochain passage")
            return None
        alerts.append(f"ENTREE FICTIVE short {j['exch']}:{j['coin']}{' (jumelle)' if j['twin'] else ''} a {px} (spread {s['spread_bp']:.1f} pb, "
                      f"glissement 1 k$ {j['slip_1k_bp']} pb, 5 k$ {j['slip_5k_bp']} pb)")
        return s, legs

    def account(ex):
        """-> (valeur, short ouvert en $, marge bloquee, ordre minimum) du compte `ex`, pour allocate()."""
        value, have, used = brokers[ex].state()
        return value, sum(-z * price(pctx[ex], c, 0.0) for c, z in have.items() if z < 0), used, MIN_LEG[ex]

    done, by_id = set(), {x["id"]: x for x in journal}
    for j in journal:                                               # 2. entrees dues, par SIGNAL : la ligne principale et sa jumelle ensemble
        if j["id"] in done or j["status"] != "signal" or now < int(j["entry_ts"]):
            continue
        if j["twin"] and by_id.get(j["twin"], {}).get("status") == "signal":
            continue                                                # traitee avec sa ligne principale
        group = [j] + [x for x in journal if x["twin"] == j["id"] and x["status"] == "signal"]
        done |= {x["id"] for x in group}
        if now > int(j["entry_ts"]) + LATE_S:
            for x in group:
                x |= {"status": "entree manquee", "note": f"passage en retard de plus de {LATE_S // 3600} h"}
                alerts.append(f"ENTREE MANQUEE {x['exch']}:{x['coin']}")
            continue
        entered = [(x, *r) for x in group if (r := paper_enter(x))]
        real = [(x, s, legs) for x, s, legs in entered if brokers.get(x["exch"]) and not fam(x)]   # un listing de perp n'est PAS la regle v1 : aucun ordre reel
        if not real:
            continue
        try:
            accounts = {x["exch"]: account(x["exch"]) for x, _, _ in real}
            if j["vol3d"] == "":
                t = int(j["t_ann"])
                j["vol3d"] = round(vol3d(candles(j["exch"], j["coin"], t - 3 * 86400, t), t) or VOL_REF, 5)
            target = F * weight(j) * sum(E for E, _, _, _ in accounts.values())      # F x w x capital des comptes ou le signal se trade
            alloc = allocate(target, accounts)
        except Exception as e:
            alerts.append(f"REPARTITION IMPOSSIBLE {j['ticker']} ({type(e).__name__}) : pas d'ordre reel pour ce signal")
            continue
        for x, s, legs in real:
            if x["exch"] not in alloc:
                x["r_note"] = f"cible {target:.0f} $ : part nulle ou sous l'ordre minimum sur ce compte (plafonds atteints), pas d'ordre"
                alerts.append(f"ENTREE {x['exch'].upper()} ECARTEE {x['coin']} : {x['r_note']}")
                continue
            cands = legs if x["exch"] == "hl" else [aster_syms[c] for c in legs if c in (aster_syms or {})]
            real_enter(x, s, pctx[x["exch"]], brokers[x["exch"]], journal, alerts, cands, target=alloc[x["exch"]])
    for j in journal:                                               # 3. stops et sorties
        if j["status"] != "ouvert":
            continue
        px_in, t_in = float(j["px_in"]), int(j["t_in"])
        if t_in >= now:                                             # ouverte a CE passage : rien n'a pu se passer, et `positions` (lu avant l'entree) ne la connait pas
            continue
        pos = positions.get(j["exch"], {})
        held = j["exch"] in positions and j["real"] == "reel"       # position reelle ouverte a un passage precedent : `pos` la connait forcement
        try:
            if held and j["r_stop"] == "" and j["coin"] in pos:     # stop absent (pose en echec) : on le repose
                try:
                    j["r_stop"] = brokers[j["exch"]].stop(j["coin"], float(j["r_size"]), float(j["r_px_in"]) * (1 + STOP))
                    alerts.append(f"STOP REPOSE sur {j['coin']}")
                except Exception as e:
                    alerts.append(f"URGENT : STOP TOUJOURS ABSENT sur {j['coin']} ({e})")
            hit = next((c for c in candles(j["exch"], j["coin"], t_in, now) if float(c["h"]) >= px_in * (1 + STOP)), None)
            gone = held and j["coin"] not in pos                    # le stop de l'exchange a rachete : la couverture ne doit pas rester seule
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
            for c, p_in in zip(legs, legs_in):
                cs = candles("hl", c, t_out - 3600, t_out)
                legs_out.append(float(cs[-1]["c"]) if cs else price(ctx, c, p_in))
            f_short = funding_sum(j["exch"], j["coin"], t_in, t_out)
            f_legs = sum(funding_sum("hl", c, t_in, t_out) for c in legs) / len(legs) if legs else 0.0
        except Exception as e:
            alerts.append(f"SORTIE FICTIVE REPORTEE {j['exch']}:{j['coin']} ({type(e).__name__} : {e}) -> nouvel essai au prochain passage")
            continue
        short, hedge, total = paper_pnl(px_in, px_out, f_short, legs_in, legs_out, f_legs, TAKER[j["exch"]])
        j |= {"status": status, "t_out": t_out, "px_out": px_out, "funding_sum": round(f_short, 6), "legs_px_out": json.dumps(legs_out), "legs_funding": round(f_legs, 6),
              "short_net": round(short, 5), "hedge_net": round(hedge, 5), "net_hedged": round(total, 5)}
        n, S, verdict = sprt_v11(journal)
        alerts.append(f"SORTIE FICTIVE ({status}) {j['exch']}:{j['coin']} : short {short * 1e4:+.0f} pb, couverture {hedge * 1e4:+.0f} pb, total {total * 1e4:+.0f} pb | "
                      f"test sequentiel (v1.1, pondere) : n {n}, S {S:+.3f}, bornes [{SPRT_DRIFT * n - SPRT_BOUND:+.2f} ; {SPRT_DRIFT * n + SPRT_BOUND:+.2f}] -> {verdict}")
    for j in journal:                                               # 4. sorties du pilote, y compris celles restees incompletes a un passage precedent
        b = brokers.get(j["exch"])
        if b and j["r_size"] != "" and j["r_net_usd"] == "" and j["status"] in ("clos", "stoppe"):
            real_exit(j, pctx[j["exch"]], b, positions.get(j["exch"], {}), alerts)
    for ex, b in brokers.items():                                   # 5. rapprochement, sur l'etat de chaque compte APRES les ordres de ce passage
        if b.live:
            want, have = expected(journal, "reel", ex), b.state()[1]
            bad = [c for c in set(want) | set(have) if abs(want.get(c, 0.0) - have.get(c, 0.0)) > 0.02 * max(abs(want.get(c, 0.0)), abs(have.get(c, 0.0)))]
            if bad:
                alerts.append(f"ECART ENTRE LE JOURNAL ET LE COMPTE {ex.upper()} sur {sorted(bad)} : a verifier a la main")


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
    try:
        actx = aster_prices()
    except Exception as e:
        print(f"prix Aster injoignables ({type(e).__name__})")
        actx = {}
    journal = read("journal.csv", J_COLS)
    first_run = not (LIVE / "announcements.csv").exists()
    brokers = {}
    for ex, mod, fn in (("hl", "listing_broker", "connect"), ("aster", "listing_broker_aster", "connect_aster")):
        try:
            b = getattr(__import__(mod), fn)()
            if b:
                brokers[ex] = b
        except Exception as e:                                                  # pas de cle, pas de ccxt, ou exchange injoignable : le suivi fictif continue
            alerts.append(f"COURTIER {ex.upper()} INDISPONIBLE ({type(e).__name__}) : suivi fictif seul sur cet exchange")
    try:
        step(now, journal, [] if first_run else fresh, alerts, ctx, aster_syms, brokers, actx)     # premier passage : on amorce l'etat sans rejouer la semaine ecoulee
    except Exception as e:                                                      # le journal est modifie en place : ce qui a ete fait (ordres compris) est ecrit quand meme
        alerts.append(f"PASSAGE INTERROMPU ({type(e).__name__}) : etat sauvegarde, reprise au prochain passage")
    write("announcements.csv", A_COLS, [dict(zip(A_COLS, r)) for r in merged])      # d'abord l'etat : plus rien apres ne doit pouvoir le perdre
    write("journal.csv", J_COLS, journal)
    values = {}
    for ex, b in brokers.items():
        try:
            v = b.state()[0]
            with open(LIVE / ("equity.csv" if ex == "hl" else f"equity_{ex}.csv"), "a", encoding="utf-8") as f:
                f.write(f"{now},{v:.2f},{'reel' if b.live else 'simulation'}\n")
            if b.live:
                values[ex] = v
        except Exception as e:
            alerts.append(f"VALEUR DU COMPTE {ex.upper()} ILLISIBLE ({type(e).__name__}) : courbe et plancher non controles a ce passage")
        days = getattr(b, "days_left", None)
        if days is not None and days < KEY_WARN_DAYS and time.gmtime(now).tm_hour == 0:     # une fois par jour : sans cle valide, plus d'entree NI de sortie
            alerts.append(f"LA CLE D'AGENT {ex.upper()} EXPIRE DANS {days:.0f} JOURS : a renouveler (secrets de la tache)")
    if len(values) == 2 and time.gmtime(now).tm_hour == 0 and max(values.values()) / sum(values.values()) > BALANCE_ALERT:
        alerts.append("COLLATERAL DESEQUILIBRE : " + ", ".join(f"{ex} {v:.0f} $" for ex, v in values.items()) +
                      f" (plus de {BALANCE_ALERT:.0%} sur un compte) -> transfert manuel conseille, les cles du pilote ne peuvent rien retirer")
    why = hard_stops(journal, values)
    if why and not (LIVE / "STOP").exists():
        (LIVE / "STOP").write_text(f"arret dur du {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(now))} : " + " ; ".join(why) + "\n", encoding="utf-8")
        alerts.append("ARRET DUR DECLENCHE : " + " ; ".join(why) + " -> plus aucune entree reelle (les sorties continuent) ; supprimer live/STOP pour reprendre")
    (LIVE / "alerts.txt").write_text("\n".join(alerts), encoding="utf-8")
    print(f"{time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(now))} | annonces connues {len(merged)} (+{len(fresh)}) | journal {len(journal)} lignes | "
          f"ouverts {sum(j['status'] == 'ouvert' for j in journal)} | signaux en attente {sum(j['status'] == 'signal' for j in journal)} | "
          f"perps Aster {len(aster_syms)} | courtiers {', '.join(ex + (' REEL' if b.live else ' simulation') for ex, b in brokers.items()) or 'aucun'}")
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
    assert sprt([0.05] * 10)[2] == "en cours" and sprt([0.05] * 40)[::2] == (33, "VALIDE") and sprt([-0.03] * 40)[::2] == (35, "ARRET")
    assert sprt([-1.4, 3.0, 3.0])[2] == "ARRET"                                                   # le premier franchissement est definitif
    assert sprt([0.01] * SPRT_NMAX)[2] == "VALIDE" and sprt([0.005] * SPRT_NMAX)[2] == "ARRET" and sprt([0.01] * (SPRT_NMAX - 1))[2] == "en cours"
    assert weight({"vol3d": "0.16"}) == 0.5 and weight({"vol3d": ""}) == 1.0 and weight({"vol3d": "0.04"}) == 1.0

    def closed(i, status="clos", net=0.0, fund=0.0, market=""):
        return {c: "" for c in J_COLS} | {"t_out": i, "status": status, "net_hedged": net, "funding_sum": fund, "market": market}
    base = [closed(i) for i in range(26)] + [closed(26 + i, "stoppe") for i in range(4)]
    assert hard_stops(base) == [] and "5 stops" in hard_stops(base[1:] + [closed(99, "stoppe")])[0]
    assert sprt_v11([closed(i, net=-0.5) | {"t_in": V11_FROM - 1} for i in range(9)])[2] == "en cours"          # v1 : hors du test de la v1.1
    assert sprt_v11([closed(i, net=-0.5) | {"t_in": V11_FROM, "twin": "x"} for i in range(9)])[2] == "en cours"  # une jumelle ne compte jamais
    m75 = 75 / LEV_SHORT + 75 / LEV_HEDGE                                                         # marge d'un short de 75 $ couvert
    near = lambda a, b: a.keys() == b.keys() and all(abs(a[k] - b[k]) < 1e-6 for k in a)
    assert near(allocate(150, {"hl": (150, 0, 0, 11), "aster": (150, 0, 0, 6)}), {"hl": 75, "aster": 75})                       # rien d'ouvert : duplication
    assert near(allocate(150, {"hl": (150, 75, m75, 11), "aster": (150, 0, 0, 6)}), {"hl": 37.5, "aster": 112.5})              # equilibrage
    assert near(allocate(150, {"hl": (150, 112.5, 1.5 * m75, 11), "aster": (150, 112.5, 1.5 * m75, 6)}), {"hl": 37.5, "aster": 37.5})   # reduit
    assert near(allocate(14, {"aster": (150, 0, 0, 6)}), {"aster": 14})                                                       # un seul exchange
    assert near(allocate(150, {"hl": (150, 145, 0, 11), "aster": (150, 0, 0, 6)}), {"aster": 150}) and allocate(10, {"hl": (150, 0, 0, 11)}) == {}
    assert sprt_v11([closed(i, net=-0.5) | {"t_in": V11_FROM} for i in range(9)])[2] == "ARRET"
    assert hard_stops(base + [closed(100, "stoppe", -0.9, market="perp")]) == []               # la famille perp n'entre ni dans le test ni dans les arrets
    assert "funding" in hard_stops([closed(i, fund=-0.03) for i in range(20)])[0] and "aster" in hard_stops(base, {"hl": 150.0, "aster": 99.0})[0]
    journal, alerts = [], []                                                                      # machine a etats, sans reseau
    step(t, journal, [{"ts": t - 3600, "venue": "upbit", "ticker": "ZAMA"}], alerts, uni, {"XYZ": "XYZUSDT"})
    assert journal[0]["status"] == "ineligible" and "Aster" in journal[0]["note"]
    step(t, journal, [{"ts": t - 45 * 3600, "venue": "upbit", "ticker": "SUI"}], alerts, uni)     # vue 45 h apres : au-dela de +36 h + LATE_S
    assert journal[1]["note"] == "annonce vue trop tard" and len(alerts) == 2

    class Fake:                                                                                   # courtier sans reseau : tient ses positions, enregistre les ordres
        def __init__(self, live):
            self.live, self.sent, self.pos, self.fail, self.part, self.lev, self.used, self.cancels = live, [], {}, set(), {}, {}, 0.0, 0
        def state(self): return 150.0, dict(self.pos), self.used
        def size_for(self, coin, usd, px): return round(usd / px, 4)
        def leverage(self, coin, lev): self.lev[coin] = lev
        def market(self, coin, side, size, ref, reduce=False):
            if (coin, side) in self.fail:
                raise RuntimeError("refus simule")
            size = round(size * self.part.pop((coin, side), 1.0), 10)                             # execution partielle simulee, une seule fois
            self.sent.append((coin, side, size, reduce))
            self.pos[coin] = round(self.pos.get(coin, 0.0) + (size if side == "buy" else -size), 10)
            self.pos = {c: z for c, z in self.pos.items() if z}
            return size, ref
        def stop(self, coin, size, trigger):
            if ("stop", coin) in self.fail:
                raise RuntimeError("stop refuse")
            return f"stop@{trigger:.2f}"
        def cancel_all(self, coin): self.cancels += 1
    ctx2 = {"BTC": {"midPx": "90000", "dayNtlVlm": "9e9"}, "ETH": {"midPx": "2000", "dayNtlVlm": "5e9"}, "SOL": {"midPx": "100", "dayNtlVlm": "4e9"},
            "DOGE": {"midPx": "0.2", "dayNtlVlm": "1e9"}, "BNB": {"midPx": "600", "dayNtlVlm": "1e9"},
            "HYPE": {"midPx": "50", "dayNtlVlm": "3e9"}, "SUI": {"midPx": "1", "dayNtlVlm": "1e8", "funding": "0"}, "XRP": {"midPx": "2", "dayNtlVlm": "2e9"}}
    book_snapshot = lambda exch, coin: {"bid": 1.0, "ask": 1.001, "mid": 1.0005, "spread_bp": 10.0, "sell": [1.0, 0.999], "buy": [1.001, 1.002]}
    candles = lambda exch, coin, a, b: [{"t": a * 1000, "T": b * 1000, "o": "1", "h": "1.01", "c": "1"}]
    funding_sum = lambda exch, coin, a, b: 0.0
    b = Fake(live=True)
    sig = {c: "" for c in J_COLS} | {"id": "x", "ticker": "SUI", "exch": "hl", "coin": "SUI", "status": "signal", "entry_ts": t, "exit_ts": t + HOLD_S, "t_ann": t - DELAY_S}
    jr, al = [sig], []
    step(t + 60, jr, [], al, ctx2, {}, b)
    assert sig["status"] == "ouvert" and sig["r_size"] == 75.0 and sig["r_net_usd"] == "" and b.pos["SUI"] == -75.0, (sig["status"], al)     # REGRESSION : ouverte a ce passage, pas refermee aussitot
    legs = json.loads(sig["r_legs"])                                                              # 50 % de 150 $ (une seule bougie : vol inconnue, pas de reduction), 4 lignes de ~18,75 $
    assert json.loads(sig["legs"]) == list(HEDGE) and [c for c, _, _ in legs] == list(HEDGE) and abs(sum(z * p for _, z, p in legs) - 75) < 0.5     # panier fixe, pas HYPE
    assert b.lev == {"SUI": 3, "ETH": 10, "SOL": 10, "DOGE": 10, "BNB": 10} and expected(jr) == b.pos and not [x for x in al if "ECART" in x]
    step(t + 3660, jr, [], al, ctx2, {}, b)
    assert sig["status"] == "ouvert"                                                              # passage suivant : la position est sur le compte, rien ne bouge
    b.fail = {("BNB", "sell")}                                                                    # sortie : la derniere ligne refuse de se vendre
    step(t + HOLD_S + 60, jr, [], al, ctx2, {}, b)
    n_buy = sum(x[:2] == ("SUI", "buy") for x in b.sent)
    assert sig["status"] == "clos" and sig["r_px_out"] != "" and sig["r_net_usd"] == "" and n_buy == 1 and b.pos == {"BNB": json.loads(sig["r_legs"])[3][1]} and b.cancels == 1
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
    b3 = Fake(live=True)                                                                          # listing de perp : fictif seulement, et ne bloque pas le spot du meme coin
    jp = [{c: "" for c in J_COLS} | {"id": "p", "ticker": "SUI", "exch": "hl", "coin": "SUI", "status": "signal", "entry_ts": t, "exit_ts": t + HOLD_S, "t_ann": t - DELAY_S, "market": "perp"}]
    step(t + 60, jp, [], [], ctx2, {}, b3)
    assert jp[0]["status"] == "ouvert" and jp[0]["r_size"] == "" and b3.sent == [] and b3.pos == {}
    step(t + 120, jp, [{"ts": t - 3600, "venue": "binance", "market": "spot", "ticker": "SUI"}], [], ctx2, {}, b3)
    assert jp[1]["note"] != "deja un signal ou une position sur ce coin" and jp[1]["market"] == "spot"
    eth = {c: "" for c in J_COLS} | {"id": "e", "ticker": "ETH", "exch": "hl", "coin": "ETH", "status": "ouvert", "legs": "[]", "r_note": ""}
    real_enter(eth, {"bid": 2000.0}, ctx2, Fake(live=True), [dict(sig) | {"r_net_usd": "", "r_px_out": "", "r_legs": json.dumps([["ETH", 0.0055, 2000]]), "r_legs_out": ""}, eth], [])
    assert eth["r_size"] == "" and "deja en portefeuille" in eth["r_note"]                        # ETH sert de couverture ailleurs : pas de short reel dessus
    x = 0.16 / 288 ** 0.5                                                                         # vol journaliere 16 % : taille divisee par 2
    candles = lambda exch, coin, a, b: [{"t": (a + 300 * i) * 1000, "o": "1", "c": str(math.exp(x if i % 2 else -x))} for i in range(864)]
    def enter(used):
        f = Fake(live=True)
        f.used = used
        jj = dict(sig) | {c: "" for c in R_COLS} | {"legs": json.dumps(["ETH", "SOL", "HYPE", "XRP"]), "vol3d": ""}
        real_enter(jj, {"bid": 1.0}, ctx2, f, [jj], [])
        return jj
    assert abs(enter(0.0)["r_size"] - 37.5) < 0.1
    assert abs(enter(95.0)["r_size"] - 10 / (1 / LEV_SHORT + 1 / LEV_HEDGE)) < 0.01             # marge : 70 % x 150 - 95 = 10 $ libres -> ~23 $ de short au plus
    low = enter(101.0)
    assert low["r_size"] == "" and "limite : marge" in low["r_note"] and "pas d'entree" in low["r_note"]
    one = json.loads(enter(97.0)["r_legs"])                                                       # ~18 $ : une seule ligne, au notionnel du short
    assert len(one) == 1 and abs(one[0][1] * one[0][2] - 8 / (1 / LEV_SHORT + 1 / LEV_HEDGE)) < 1
    assert abs(vol3d(candles("hl", "Z", t - 3 * 86400, t), t) - 0.16) < 0.001
    capped = Fake(live=True)                                                                      # plafond d'exposition : 140 $ deja shortes sur 150 $
    capped.pos = {"ZZZ": -140.0}
    jc = dict(sig) | {c: "" for c in R_COLS} | {"legs": json.dumps(["ETH"]), "vol3d": ""}
    real_enter(jc, {"bid": 1.0}, ctx2 | {"ZZZ": {"midPx": "1"}}, capped, [jc], [])
    assert jc["r_size"] == "" and "limite : plafond d'exposition" in jc["r_note"] and capped.sent == []
    candles = lambda exch, coin, a, b: [{"t": a * 1000, "T": b * 1000, "o": "1", "h": "1.01", "c": "1"}]
    b4 = Fake(live=True)                                                                          # executions partielles a la sortie
    s4 = dict(sig) | {c: "" for c in R_COLS} | {"status": "signal", "t_in": "", "net_hedged": "", "vol3d": ""}
    j4 = [s4]
    step(t + 60, j4, [], [], ctx2, {}, b4)
    lsz = {c: z for c, z, _ in json.loads(s4["r_legs"])}
    b4.part = {("SUI", "buy"): 0.4, ("ETH", "sell"): 0.5}
    a4 = []
    step(t + HOLD_S + 60, j4, [], a4, ctx2, {}, b4)
    assert s4["r_px_out"] == "" and b4.cancels == 0 and b4.pos["SUI"] == -45.0 and expected(j4) == b4.pos and not [x for x in a4 if "ECART" in x]    # le stop reste
    step(t + HOLD_S + 3660, j4, [], [], ctx2, {}, b4)
    assert s4["r_px_out"] != "" and b4.cancels == 1 and s4["r_net_usd"] == "" and abs(b4.pos["ETH"] - lsz["ETH"] / 2) < 1e-9 and expected(j4) == b4.pos
    step(t + HOLD_S + 7260, j4, [], [], ctx2, {}, b4)
    assert s4["r_net_usd"] != "" and b4.pos == {} and abs(sum(z for c, sd, z, _ in b4.sent if (c, sd) == ("ETH", "sell")) - lsz["ETH"]) < 1e-9
    assert sum(z for c, sd, z, _ in b4.sent if (c, sd) == ("SUI", "buy")) == 75.0                 # jamais rachete deux fois
    b5, a5 = Fake(live=False), []                                                                 # HL_LIVE repasse a 0 avec une position reelle ouverte
    real_exit(dict(s4) | {"r_px_out": "", "r_net_usd": "", "r_legs_out": "", "r_out": "", "real": "reel"}, ctx2, b5, {}, a5)
    assert b5.sent == [] and "EN ATTENTE" in a5[0]

    bh, ba = Fake(live=True), Fake(live=True)                                                   # Aster : compte, couverture et rapprochement propres
    syms = {"ETH": "ETHUSDT", "SOL": "SOLUSDT", "DOGE": "DOGEUSDT", "BNB": "BNBUSDT", "Q": "QUSDT"}
    actx = {k: {"midPx": v} for k, v in (("ETHUSDT", "2000"), ("SOLUSDT", "100"), ("DOGEUSDT", "0.2"), ("BNBUSDT", "600"), ("QUSDT", "1"))}
    sa = {c: "" for c in J_COLS} | {"id": "q", "ticker": "Q", "exch": "aster", "coin": "QUSDT", "status": "signal", "entry_ts": t, "exit_ts": t + HOLD_S, "t_ann": t - DELAY_S,
                                    "vol3d": "0.08", "venue": "bithumb", "market": "spot_krw"}
    ref_price, lf.ref_price = lf.ref_price, lambda venue, market, ticker: refs.get(ticker)     # identite : prix du lieu fige, sans reseau
    refs = {"Q": None}
    ja, aa = [sa], []
    step(t + 60, ja, [], aa, ctx2, syms, {"hl": Fake(live=True), "aster": Fake(live=True)}, actx)
    assert sa["status"] == "signal" and "identite non verifiable" in aa[-1]                       # pas encore de prix sur le lieu : on attend
    sb, refs["Q"] = dict(sa), 3.0
    step(t + 60, [sb], [], aa, ctx2, syms, {"hl": Fake(live=True), "aster": Fake(live=True)}, actx)
    assert sb["status"] == "ineligible" and "identite douteuse" in sb["note"]                      # prix Aster = 1/3 du prix liste : autre actif
    refs["Q"], aa = 1.0, []
    step(t + 60, ja, [], aa, ctx2, syms, {"hl": bh, "aster": ba}, actx)
    assert bh.sent == [] and sa["r_size"] == 75.0 and [c for c, _, _ in json.loads(sa["r_legs"])] == ["ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT"], aa
    assert expected(ja, "reel", "aster") == ba.pos and expected(ja, "reel", "hl") == {}
    step(t + 3660, ja, [], aa, ctx2, syms, {"hl": bh, "aster": ba}, actx)
    assert not [x for x in aa if "ECART" in x]
    step(t + HOLD_S + 60, ja, [], aa, ctx2, syms, {"hl": bh, "aster": ba}, actx)
    assert sa["r_net_usd"] != "" and ba.pos == {} and bh.sent == []
    hh, aa2, refs["SUI"] = Fake(live=True), Fake(live=True), 1.0                                   # signal sur les deux exchanges : duplication et ligne jumelle
    syms2 = syms | {"SUI": "SUIUSDT"}
    actx2 = actx | {"SUIUSDT": {"midPx": "1"}}
    flat = lambda exch, coin, a, b: [{"t": (t - 3 * 86400 + 300 * i) * 1000, "T": (t - 3 * 86400 + 300 * i + 300) * 1000, "o": "1", "h": "1.001",
                                      "c": "1.001" if i % 2 else "0.999"} for i in range(864)]
    candles, jd, ad = flat, [], []
    step(t + 60, jd, [{"ts": t, "venue": "binance", "market": "spot", "ticker": "SUI"}], ad, ctx2, syms2, {"hl": hh, "aster": aa2}, actx2)
    assert [(x["exch"], x["coin"], x["status"], x["twin"]) for x in jd] == [("hl", "SUI", "signal", ""), ("aster", "SUIUSDT", "signal", jd[0]["id"])], ad
    step(int(jd[0]["entry_ts"]), jd, [], ad, ctx2, syms2, {"hl": hh, "aster": aa2}, actx2)
    assert hh.pos.get("SUI") == -75.0 and aa2.pos.get("SUIUSDT") == -75.0 and [c for c, _, _ in json.loads(jd[1]["r_legs"])] == ["ETHUSDT", "SOLUSDT", "DOGEUSDT", "BNBUSDT"], ad
    step(int(jd[0]["entry_ts"]) + HOLD_S + 60, jd, [], ad, ctx2, syms2, {"hl": hh, "aster": aa2}, actx2)
    assert all(x["r_net_usd"] != "" for x in jd) and hh.pos == {} and aa2.pos == {} and len(v1_closed(jd)) == 1      # deux trades reels, un seul signal
    lf.ref_price = ref_price

    def candles_gone(exch, coin, a, b):
        if coin == "GONE":
            raise RuntimeError("perp radie")
        return [{"t": a * 1000, "T": b * 1000, "o": "1", "h": "1.01", "c": "1"}]
    candles = candles_gone                                                                        # une ligne en erreur ne bloque pas les autres
    row = lambda i, coin: {c: "" for c in J_COLS} | {"id": i, "ticker": coin, "exch": "hl", "coin": coin, "status": "ouvert", "t_ann": t - DELAY_S, "t_in": t,
                                                     "px_in": 1.0, "legs": "[]", "legs_px_in": "[]", "exit_ts": t + 10}
    jg, ag = [row("g", "GONE"), row("o", "SUI")], []
    step(t + 100, jg, [], ag, ctx2, {}, None)
    assert jg[0]["status"] == "ouvert" and jg[1]["status"] == "clos" and any("REPORTEE" in x for x in ag)
    print("self-check OK")


if __name__ == "__main__":
    _selftest() if "--test" in sys.argv else main()
