"""Annonces de listing spot des grands exchanges, normalisees. Les annonces sont du TEXTE SEUL ; seuls ref_price / same_asset / aster_crypto
lisent un prix ou une fiche de contrat, pour verifier avant un ordre que le perp est bien le coin annonce (collisions de tickers).

    .venv/Scripts/python listing_feed.py          # -> data/listings/announcements.parquet (+ caches des API officielles)
    .venv/Scripts/python listing_feed.py --test   # analyseurs sur messages figes, sans reseau

Une source par lieu, la plus proche de l'original :
  upbit      API d'avis officielle, horodatage de premiere publication
  binance    API CMS officielle, rubrique "New Cryptocurrency Listing"
  bithumb    API d'avis officielle en direct (5 derniers avis seulement) EN PLUS des fils : source de premiere main, plus rapide, redondante
  coinbase, robinhood
             titres des depeches de quatre fils chinois (corpus de news.py) : Coinbase et Robinhood annoncent sur X.
             L'heure est celle de la premiere depeche : quelques minutes de retard, sans effet sur une entree a +36 h.
Une annonce = (lieu, ticker). Les redites du meme couple dans les DEDUP_DAYS jours (rappel du jour J, ouverture des echanges) sont ignorees.
Les API officielles sont mises en cache et relues par pages jusqu'au premier avis deja connu (Upbit repond 429 au-dela de quelques pages par minute).
"""
import datetime as dt
import html
import json
import pathlib
import re
import sys
import time

import requests

OUT = pathlib.Path("data/listings")
NEWS = pathlib.Path("data/impulse/news")
WIRES = ("theblockbeats", "ForesightNews", "Odaily_News", "chaincatcher")
START = "2023-01-01"
DEDUP_DAYS = 14
UA = {"User-Agent": "Mozilla/5.0"}
TRIES = 8                                                       # listing_live.py le baisse : une source en panne ne doit pas bloquer un passage horaire
UPBIT = "https://api-manager.upbit.com/api/v1/announcements"
BINANCE = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
MARKETS = {"upbit": "https://api.upbit.com/v1/market/all", "bithumb": "https://api.bithumb.com/v1/market/all"}   # listes de marches : PUBLIQUES et non bloquees
BITHUMB = "https://api.bithumb.com/v1/notices"        # n'expose que les 5 derniers avis : suivi en direct seulement, aucun historique
NOT_A_COIN = {"KRW", "BTC", "USDT", "USD", "USDC", "ETH", "EUR", "BNB", "FDUSD", "TRY", "BRL", "JPY", "ETF", "NFT", "IPO", "CEO", "SEC", "DEX", "CEX", "API", "APP", "UTC",
              "KST", "APR", "APY", "US", "EU", "UK", "U", "V2", "V3", "L2", "DAO", "TVL", "USDE", "BUSD", "TUSD", "DAI", "BSC", "ERC", "SPL", "BEP", "ERC20", "BEP20"}
TICKER = r"[A-Z0-9]{1,12}"
WIRE_HEAD = re.compile(r"^(?:据官方公告，|据官方消息，|韩国|第二大|最大|加密货币|加密|交易所|交易平台|\s)*(Upbit|Bithumb|Coinbase|Robinhood)\s*(?:Crypto|Markets|Assets)?\s*(?:宣布|称|：|:)?\s*"
                       r"(将于今日|将于[^上，]{0,14}|将在[^上，]{0,14}|即将|计划|将|新增|现已|已|正式)?\s*(?:上线|上架)", re.I)
WIRE_BAD = re.compile(r"永续|合约|期货|期权|杠杆|下架|下线|终止|路线图|国际|International|衍生|钱包|Wallet|质押|借贷|贷款|股票|ETF|预测|Chain|链上|功能|活动|空投|储备|指数|理财|Earn|应用|版本|影响|转账|充提|充值|提现", re.I)
PERP_BAD = re.compile(r"(?i)equity|tradfi|stock|index|pre-market|premarket|pre-ipo|commodit|gold|silver|quarterly|delivery|coin-m")
WIRE_MARKET = {"bithumb": "spot_krw", "coinbase": "spot", "robinhood": "spot"}       # Bithumb ne liste presque que contre le won : meme convention que l'echantillon
KRW_TICKER = {"upbit": "https://api.upbit.com/v1/ticker", "bithumb": "https://api.bithumb.com/v1/ticker"}
BINANCE_BOOK = "https://data-api.binance.vision/api/v3/ticker/bookTicker"           # api.binance.com repond 451 depuis les serveurs GitHub (Etats-Unis)
COINBASE = "https://api.exchange.coinbase.com/products"
ASTER_CRYPTO_SUB = {"Top", "Meme", "AI"}       # sous-types Aster des perps crypto (2026-10-06) ; tout autre (STOCK, ETF, Commodities, AOS2, USD1-RWA, pre-launch, inconnu) = exclu
SAME_BAND = 1.5     # meme actif si 1/1,5 <= p_perp / p_ref <= 1,5. Mesure 2026-10-06 sur 106 listings recents (Upbit, Bithumb, Coinbase, Binance, Robinhood), heure t+36 h,
                    # contre le perp HL ou Binance : |ecart| median 0,3 %, p90 1,5 %, max sain x1,125 (XCN, prime coreenne) ; plus petite vraie collision x2,0
                    # (AI : Gensyn sur Upbit, Sleepless AI en perp). 1,5 = leur milieu geometrique. MANTRA n'est PAS une collision (meme coin que Binance spot) :
                    # prime isolee x1,12 sur Upbit, x1,6 sur Bithumb, coin sous avertissement "ecart de prix mondial" ; rejete a x1,6 = trade rate, accepte.


def tickers(text):
    """Tickers d'un titre : ceux entre parentheses s'il y en a (listes a virgules comprises), sinon les mots en capitales."""
    inside = [t.strip().lstrip("$") for grp in re.findall(r"[（(]([^（()）]*)[)）]", text) for t in re.split(r"[,，、]", grp)]
    found = [t for t in inside if re.fullmatch(TICKER, t)] or re.findall(rf"(?<![A-Za-z0-9]){TICKER}(?![A-Za-z0-9])", text)
    out = []
    for t in found:
        if t not in NOT_A_COIN and not t.isdigit() and t not in out:
            out.append(t)
    return out


def parse_wire(text):
    """Depeche d'un fil chinois -> (lieu, marche, [tickers]) si son TITRE annonce un listing spot, sinon None."""
    title = text.split("\n", 1)[0].strip()
    title = re.sub(r"^(?:🔔重要快讯|⚡️|⚡)\s*", "", title)
    title = re.sub(r"\s*-\s*链接$", "", title).strip("【】 ")
    m = WIRE_HEAD.match(title)
    if not m or WIRE_BAD.search(title):
        return None
    venue, modal = m.group(1).lower(), m.group(2) or ""
    will = modal.startswith(("将", "即将", "计划"))
    if venue == "coinbase" and not will:                        # "上线" / "已上线" / "正式上线" : ouverture des echanges, pas l'annonce
        return None
    if venue == "bithumb" and modal in ("现已", "已", "正式"):
        return None                                             # Robinhood liste sans preavis : toutes les formes sont l'annonce
    if venue == "upbit":                                        # secours de l'API officielle (elle refuse les serveurs de la tache horaire)
        if modal in ("现已", "已", "正式"):
            return None
        market = "spot_krw" if re.search("韩元|KRW|원화", text) else "spot"      # mention explicite du won : rappel 96 %, precision 98 % contre l'API (sans l'exiger : 99 % / 80 %)
    else:
        market = WIRE_MARKET[venue]
    tk = tickers(title[m.end():])
    return (venue, market, tk) if tk else None


def parse_upbit(title):
    """Avis Upbit -> (marche, [tickers]) pour un ajout de marche ou une nouvelle cotation, sinon None."""
    head = re.sub(r"\([^()]*안내[^()]*\)", "", title)                                    # parentheses finales = rappels ("... 연기 안내"), pas des tickers
    if not ("디지털 자산 추가" in head or "신규 거래지원" in head) or re.search("종료|유의", head):
        return None
    tk = tickers(head)
    return ("spot_krw" if "KRW" in head or "원화" in head else "spot", tk) if tk else None


def parse_bithumb(title):
    """Avis Bithumb -> [tickers] si c'est un ajout au marche en won, sinon None. Format officiel : "[마켓 추가] 트라발라(AVA) 원화 마켓 추가"."""
    if "원화" not in title or "마켓" not in title or not re.search(r"추가|개시", title) or re.search(r"종료|중지|유의|이벤트|에어드랍", title):
        return None
    return tickers(re.sub(r"^\s*\[[^\]]*\]\s*", "", title)) or None


def bithumb_page(page=1):
    """-> ([avis], False) ; meme forme que les autres sources, mais sans pagination possible."""
    r = requests.get(BITHUMB, params={"count": 100}, headers=UA, timeout=30)
    r.raise_for_status()
    return r.json(), False


def krw_markets(venue):
    """Tickers cotes contre le won sur `venue`, vus par son API de marches publique. Sert de DETECTEUR de secours :
    l'API d'avis d'Upbit renvoie 403 depuis les serveurs GitHub (verifie le 2026-09-22), celle-ci repond."""
    r = requests.get(MARKETS[venue], headers=UA, timeout=30)
    r.raise_for_status()
    out = {m["market"].split("-", 1)[1] for m in r.json() if str(m.get("market", "")).startswith("KRW-")}
    if len(out) < 50:
        raise RuntimeError(f"liste de marches {venue} suspecte ({len(out)} lignes)")      # reponse tronquee : ne jamais l'interpreter comme des retraits
    return out


def aster_crypto(s):
    """Element de GET /fapi/v1/exchangeInfo (champ "symbols") -> True si c'est un perp sur une CRYPTO.
    underlyingType vaut "COIN" partout (METAUSDT = l'action Meta compris) : inutile. Regle, liste blanche : symbolType 0 (les 138 symboles a 1 sont
    actions, ETF, matieres premieres, change, pre-IPO) ET sous-types tous dans ASTER_CRYPTO_SUB. Un sous-type nouveau exclut : rater un trade plutot qu'un mauvais actif."""
    return (s.get("contractType") == "PERPETUAL" and s.get("symbolType") == 0
            and set(s.get("underlyingSubType") or ()) <= ASTER_CRYPTO_SUB)


def lot(name):
    """Unites de coin par unite cotee, d'apres le prefixe : 1000PEPEUSDT, kPEPE (Hyperliquid), 1000CHEEMS -> 1000 ; 1MBABYDOGE -> 1e6 ; PEPE, 1INCH -> 1."""
    m = re.match(r"(1000000|10000|1000|1M|k)(?=[A-Z])", name)
    return {"1000000": 1e6, "10000": 1e4, "1000": 1e3, "1M": 1e6, "k": 1e3}[m.group(1)] if m else 1


def _px_json(url, **params):
    """Un GET de prix public -> json, ou None (symbole inconnu, ou API muette apres 3 essais)."""
    for k in range(3):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):                     # symbole inconnu ou retire : definitif
                return None
        except (requests.RequestException, ValueError):
            pass
        time.sleep(2 * (k + 1))
    return None


def _mid(bid, ask):
    b, a = float(bid), float(ask)
    return (a + b) / 2 if a > 0 and b > 0 else None              # carnet vide = paire retiree (Binance garde un dernier prix fige, ex. XMRUSDT)


def ref_price(venue, market, ticker):
    """Prix en $ d'UNE unite de `ticker` (tel qu'ecrit dans l'annonce) SUR LE LIEU QUI L'A LISTE, ou None si introuvable.
      upbit, bithumb  dernier prix KRW-<T> / KRW-USDT du MEME lieu (la prime coreenne generale s'annule). Toujours le marche won, quel que soit `market` ;
                      None si le marche n'existe pas ou n'a pas encore d'echange (Bithumb : trade_price null avant l'ouverture).
      binance         milieu du carnet <T>USDT (data-api.binance.vision). Une annonce de perp (market "perp") n'a souvent pas de spot : None.
      coinbase        milieu du carnet <T>-USD, sinon <T>-USDC.
      robinhood       pas d'API de prix publique : prix Coinbase, sinon Binance. Si les deux existent et se contredisent (same_asset faux), le ticker
                      est ambigu : None. Limite : on verifie l'actif de Coinbase / Binance, pas celui de Robinhood.
    Le moteur compare avec same_asset(ref, prix du perp, lot(symbole du perp) / lot(ticker)) : PEPE sur Upbit contre 1000PEPEUSDT -> mult 1000,
    1000CHEEMS sur Binance contre 1000CHEEMSUSDT ou kCHEEMS -> mult 1. None = identite non verifiable : pas d'ordre reel."""
    t = ticker.upper()
    try:
        if venue in KRW_TICKER:
            d = {x["market"]: float(x["trade_price"]) for x in _px_json(KRW_TICKER[venue], markets=f"KRW-{t},KRW-USDT")}
            return d[f"KRW-{t}"] / d["KRW-USDT"]
        if venue == "binance":
            d = _px_json(BINANCE_BOOK, symbol=t + "USDT")
            return _mid(d["bidPrice"], d["askPrice"])
        if venue == "coinbase":
            for q in ("USD", "USDC"):
                d = _px_json(f"{COINBASE}/{t}-{q}/ticker")
                if d:
                    return _mid(d["bid"], d["ask"])
            return None
        if venue == "robinhood":
            ps = [p for p in (ref_price("coinbase", market, t), ref_price("binance", market, t)) if p]
            return ps[0] if len(ps) == 1 or ps and same_asset(ps[0], ps[1])[0] else None
    except (TypeError, KeyError, ValueError, ZeroDivisionError):    # reponse d'erreur (Bithumb : 200 + {"error"}), champ vide, prix nul
        return None
    return None


def same_asset(p_ref, p_perp, mult=1):
    """-> (meme actif ?, ratio p_perp / (mult x p_ref)). mult = lot(symbole du perp) / lot(ticker de l'annonce). Bande : SAME_BAND."""
    if not (p_ref and p_perp) or min(p_ref, p_perp) <= 0:
        return False, None
    r = p_perp / (mult * p_ref)
    return 1 / SAME_BAND <= r <= SAME_BAND, r


def kst_epoch(s):
    """Horodatage Bithumb "2026-09-21 17:13:17", en heure de Seoul (UTC+9), sans fuseau indique -> epoch UTC."""
    return int(dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone(dt.timedelta(hours=9))).timestamp())


def parse_binance(title):
    """Article Binance -> [tickers] si c'est une annonce de listing spot ("Will List"), sinon None. Meme regle que l'echantillon."""
    h = title.lower()
    if "will delist" in h or "delisting of" in h or "futures will launch" in h or "perpetual contract" in h or "monitoring tag" in h:
        return None
    if "binance futures" in h or "binance options" in h or "contracts" in h:
        return None
    if "will list" in h or ("will add" in h and "seed tag" in h):
        return [t for t in tickers(title) if f"({t})" in title] or None        # tickers entre parentheses seulement, comme dans l'echantillon
    return None


def parse_binance_perp(title):
    """Article Binance -> [tickers] si c'est le lancement d'un perp CRYPTO ("Binance Futures Will Launch USDⓈ-Margined XUSDT ... Perpetual"), sinon None.
    Exclus : perps d'actions, d'indices, TradFi, pre-market / pre-IPO, contrats a livraison. Suivi FICTIF seulement (spec 18)."""
    h = title.lower()
    if "futures will launch" not in h or "perpetual" not in h or PERP_BAD.search(title):
        return None
    out = []
    for t in re.findall(r"\b([A-Z0-9]{2,20})USD[TC]\b", title):
        t = re.sub(r"^(1000000|10000|1000|1M)", "", t)
        if t not in NOT_A_COIN and t not in out:
            out.append(t)
    return out or None


def dedup(rows):
    """rows (ts, lieu, marche, ticker, ...) -> sans les redites du meme (lieu, ticker) dans les DEDUP_DAYS jours qui suivent une annonce retenue.
    Les annonces de perp sont dedoublonnees a part : un listing de perp ne doit jamais masquer un listing spot du meme coin (seul le spot porte la regle v1)."""
    last, out = {}, []
    for r in sorted(rows):
        k = (r[1], r[2] == "perp", r[3])
        if k not in last or r[0] - last[k] > DEDUP_DAYS * 86400:
            out.append(r)
            last[k] = r[0]
    return out


def get(url, params):
    for k in range(TRIES):
        r = requests.get(url, params=params, headers=UA, timeout=30)
        if r.status_code == 200 and r.text[:1] == "{":
            return r.json()
        if k < TRIES - 1:
            time.sleep(15 * (k + 1))                            # 429 : on attend, on ne force pas
    raise SystemExit(f"source indisponible : {url}")


def wire_page(channel, before=None):
    """Une page publique t.me/s/<canal> (~20 messages, les plus recents ou ceux d'avant `before`) -> [(msg_id, ts, texte)]. Version courte de news.parse_page."""
    r = requests.get(f"https://t.me/s/{channel}", params={"before": before} if before else None, headers=UA, timeout=30)
    r.raise_for_status()
    out = []
    for block in r.text.split('<div class="tgme_widget_message_wrap')[1:]:
        mid = re.search(r'data-post="[^"/]+/(\d+)"', block)
        ts = re.search(r'class="tgme_widget_message_date"[^>]*>\s*<time[^>]*datetime="([^"]+)"', block)
        t = re.search(r'class="tgme_widget_message_text js-message_text"[^>]*>(.*?)</div>', block, re.S)
        if mid and ts and t:
            text = html.unescape(re.sub(r"<[^>]+>", "", re.sub(r"<br\s*/?>", chr(10), t.group(1)))).strip()
            out.append((int(mid.group(1)), int(dt.datetime.fromisoformat(ts.group(1)).timestamp()), text))
    return out


def refresh(name, key, page_of):
    """Cache json d'une API paginee du plus recent au plus ancien : lit des pages jusqu'a retomber sur un avis connu."""
    path = OUT / f"{name}.json"
    known = {str(x[key]): x for x in json.loads(path.read_text(encoding="utf-8"))} if path.exists() else {}
    page = 1
    while True:
        items, more = page_of(page)
        fresh = [x for x in items if str(x[key]) not in known]
        known.update({str(x[key]): x for x in fresh})
        if not more or len(fresh) < len(items):
            break
        page += 1
        time.sleep(1.2)
    path.write_text(json.dumps(list(known.values()), ensure_ascii=False), encoding="utf-8")
    return list(known.values())


def upbit_page(page):
    d = get(UPBIT, {"os": "web", "page": page, "per_page": 20, "category": "trade"})["data"]
    return d["notices"], page < d["total_pages"]


def binance_page(page):
    arts = get(BINANCE, {"type": 1, "catalogId": 48, "pageNo": page, "pageSize": 20})["data"]["catalogs"][0]["articles"]
    return arts, bool(arts) and arts[-1]["releaseDate"] >= dt.datetime.fromisoformat(START).replace(tzinfo=dt.timezone.utc).timestamp() * 1000


def main():
    import duckdb                                               # seulement ici : listing_live.py reutilise ce module avec requests seul
    OUT.mkdir(parents=True, exist_ok=True)
    t0 = dt.datetime.fromisoformat(START).replace(tzinfo=dt.timezone.utc).timestamp()
    rows = []
    for n in refresh("upbit_notices", "id", upbit_page):
        ts, got = int(dt.datetime.fromisoformat(n["first_listed_at"]).timestamp()), parse_upbit(n["title"])
        if got and ts >= t0:
            rows += [(ts, "upbit", got[0], t, len(got[1]), n["title"][:200], "upbit_api") for t in got[1]]
    for a in refresh("binance_articles", "id", binance_page):
        ts, tk = a["releaseDate"] // 1000, parse_binance(a["title"])
        if tk and ts >= t0:
            rows += [(ts, "binance", "spot", t, len(tk), a["title"][:200], "binance_cms") for t in tk]
    con = duckdb.connect()
    files = ", ".join(f"'{(NEWS / f'{c}.parquet').as_posix()}'" for c in WIRES)
    for ts, channel, text in con.execute(f"SELECT ts::BIGINT, channel, text FROM read_parquet([{files}], union_by_name = true) WHERE ts >= {int(t0)} ORDER BY ts").fetchall():
        got = parse_wire(text)
        if got:
            rows += [(ts, got[0], got[1], t, len(got[2]), text.split("\n", 1)[0][:200], channel) for t in got[2]]
    rows = dedup(rows)
    con.execute("CREATE TABLE a (ts BIGINT, venue VARCHAR, market VARCHAR, ticker VARCHAR, n_tickers INT, head VARCHAR, source VARCHAR)")
    con.executemany("INSERT INTO a VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(f"COPY (SELECT * FROM a ORDER BY ts, venue, ticker) TO '{(OUT / 'announcements.parquet').as_posix()}' (FORMAT parquet)")
    for r in con.execute("SELECT venue, market, count(*), min(make_timestamp(ts * 1000000))::DATE::VARCHAR, max(make_timestamp(ts * 1000000))::DATE::VARCHAR FROM a GROUP BY ALL ORDER BY 1, 2").fetchall():
        print(r)


def _selftest():
    w = parse_wire
    assert w("【Bithumb 将上线 Lisk 代币 LSK 的韩元交易对】\nForesight News 消息") == ("bithumb", "spot_krw", ["LSK"])
    assert w("🔔重要快讯 Bithumb将上线Excella (WAXL)、Yield Guild Games（YGG）韩元市场\n查看原文") == ("bithumb", "spot_krw", ["WAXL", "YGG"])
    assert w("🔔重要快讯 Bithumb将于今日上线NEO和GAS韩元交易对") == ("bithumb", "spot_krw", ["NEO", "GAS"])
    assert w("⚡️Bithumb将上线Definitive (EDGE)\n\nBlockBeats 消息，支持 KRW、BTC 和 USDT 交易对") == ("bithumb", "spot_krw", ["EDGE"])
    assert w("Coinbase将上线Tensor (TNSR) - 链接") == ("coinbase", "spot", ["TNSR"])
    assert w("【Coinbase 将上线 KAIO 现货交易】\nForesight News 消息") == ("coinbase", "spot", ["KAIO"])
    assert w("【Coinbase 将上线 OPN 永续期货合约】") is None                                   # perp
    assert w("🔔重要快讯 Coinbase正式上线带有“实验资产”标签的SUI交易服务") is None              # ouverture des echanges, pas l'annonce
    assert w("Coinbase将EDGEX(EDGEX)加入上市路线图") is None                                  # feuille de route
    assert w("🔔重要快讯 Coinbase国际将上线ADA、LINK、DOGE和XLM永续期货合约") is None
    assert w("受「上线Upbit」消息影响，ID短时上涨近40% - 链接") is None                          # reaction de prix, pas une annonce
    assert w("受Coinbase上线消息影响，KARRAT短时拉升49% - 链接") is None
    assert w("🔔重要快讯 Upbit将上线SPACE ID（ID）韩元交易对") == ("upbit", "spot_krw", ["ID"])
    assert w("【Upbit 将上线 ALT 和 PYTH 交易对】\nForesight News 消息，韩国加密交易平台 Upbit 将上线 AltLayer（ALT）和 Pyth Network（PYTH）交易对") == ("upbit", "spot", ["ALT", "PYTH"])    # won non mentionne : ecarte
    assert w("【加密交易所 Upbit 将上线 STG，支持比特币交易对】") == ("upbit", "spot", ["STG"])             # marche BTC seul : hors regle
    assert w("【Upbit 将上线 NEXO USDT 交易对】") == ("upbit", "spot", ["NEXO"])
    assert w("【Robinhood 上线 Solana 生态代币 ORCA、RAY】") == ("robinhood", "spot", ["ORCA", "RAY"])
    assert w("【Robinhood 上线 W】") == ("robinhood", "spot", ["W"])
    assert w("【Robinhood 新增上线 Injective（INJ）】") == ("robinhood", "spot", ["INJ"])
    assert w("Robinhood已上线11支现货比特币ETF的交易服务 - 链接") is None
    assert w("🔔重要快讯 Robinhood Crypto在部分欧盟管辖区上线USDC") is None
    assert w("Robinhood Crypto现已上线SHIB转账") is None                                        # transferts, pas une cotation
    assert w("Coinbase将上线BNB，仅支持BSC网络") is None                                        # BNB et BSC ne sont pas des tickers de coin liste
    assert w("Coinbase将于10月9日上线Linea（LINEA）、Noice（NOICE）和Syndicate（SYND）现货") == ("coinbase", "spot", ["LINEA", "NOICE", "SYND"])
    assert w("🛰FN速递 🛰\n【dYdX 换马甲再创业，这次联手 Robinhood 能成吗？】") is None
    u = parse_upbit
    assert u("KRW, BTC 마켓 디지털 자산 추가 (ALT, PYTH)") == ("spot_krw", ["ALT", "PYTH"])
    assert u("BTC 마켓 디지털 자산 추가 (STG)") == ("spot", ["STG"])
    assert u("빅타임(BIGTIME), 아카시네트워크(AKT) 신규 거래지원 안내 (KRW, BTC, USDT 마켓) (AKT 거래지원 개시 시점 연기 안내)") == ("spot_krw", ["BIGTIME", "AKT"])
    assert u("KRW 마켓 디지털 자산 추가 (ASTR) (거래지원 개시 시점 연기 안내)") == ("spot_krw", ["ASTR"])
    assert u("플레이댑(PDA) 거래지원 종료 안내 (3/25 14:00)") is None and u("멀티버스엑스(EGLD) 거래 유의 종목 지정 안내") is None
    bt = parse_bithumb
    assert bt("[마켓 추가] 트라발라(AVA) 원화 마켓 추가") == ["AVA"]
    assert bt("[마켓 추가] 인터폴드(FOLD), 이유알코인(EURC) 원화 마켓 추가") == ["FOLD", "EURC"]
    assert bt("[마켓 추가] 블록스트리트(BSB) 원화 마켓 추가") == ["BSB"]
    assert bt("페치(FET), 멀티버스엑스(EGLD), 어크로스프로토콜(ACX) 거래유의종목 지정") is None        # surveillance, pas un listing
    assert bt("소닉(S) 입출금 일시 중지 안내 (09/21 오후 8시~)") is None and bt("9월 3주차 가스(GAS) 에어드랍 지급 안내") is None
    assert bt("[이벤트] 총 3억원 상당, 젠신(AI) 원화마켓 추가 기념 이벤트") is None                     # evenement marketing autour d'un listing
    assert kst_epoch("2026-09-21 09:00:00") == kst_epoch("2026-09-21 00:00:00") + 9 * 3600 and dt.datetime.fromtimestamp(kst_epoch("2026-01-01 09:00:00"), dt.timezone.utc).hour == 0
    bp = parse_binance_perp
    assert bp("Binance Futures Will Launch USDⓈ-Margined JELLYJELLYUSDT and MAVIAUSDT Perpetual Contracts") == ["JELLYJELLY", "MAVIA"]
    assert bp("Binance Futures Will Launch USDⓈ-Margined KDAUSDT Perpetual Contract With up to 75x Leverage") == ["KDA"]
    assert bp("Binance Futures Will Launch USDⓈ-Margined 1000CHEEMSUSDT Perpetual Contract") == ["CHEEMS"]
    assert bp("Binance Futures Will Launch USDⓈ-Margined INTCUSDT and HOODUSDT Equity Perpetual Contracts (2026-04-01)") is None
    assert bp("Binance Futures Will Launch MOONSHOTUSDT USDⓈ-Margined Pre-IPO Perpetual Contract") is None
    assert bp("Binance Futures Will Launch USDBRLUSDT USDⓈ-Margined TradFi Perpetual Contract (2026-09-21)") is None
    assert bp("Binance Will List Jito (JTO) with Seed Tag Applied") is None and parse_binance("Binance Futures Will Launch USDⓈ-Margined KDAUSDT Perpetual Contract") is None
    d2 = dedup([(0, "binance", "perp", "X"), (86400, "binance", "spot", "X"), (2 * 86400, "binance", "perp", "X")])
    assert [(r[0], r[2]) for r in d2] == [(0, "perp"), (86400, "spot")]                      # le perp ne masque pas le spot ; la redite de perp est ecartee
    b = parse_binance
    assert b("Binance Will List Jito (JTO) with Seed Tag Applied") == ["JTO"]
    assert b("Binance Futures Will Launch USDⓈ-Margined PLUMEUSDT Perpetual Contract") is None
    assert b("Binance Futures Will List USDⓈ-M & COIN-M Quarterly 1227 Delivery Contracts") is None and b("Binance Will List BFUSD and Introduce BFUSD Zero Trading Fee Promotion") is None
    assert b("Binance Will List Cheems (1000CHEEMS) and Test (TST) with Seed Tag Applied") == ["1000CHEEMS", "TST"]
    assert b("Binance Will Add Plume (PLUME) on Earn, Buy Crypto, Convert & Margin") is None and b("Binance Will Delist ABC (ABC)") is None
    d = dedup([(0, "bithumb", "spot_krw", "X"), (86400, "bithumb", "spot_krw", "X"), (86400, "upbit", "spot_krw", "X"), (20 * 86400, "bithumb", "spot_krw", "X")])
    assert [r[0] for r in d] == [0, 86400, 20 * 86400] and d[1][1] == "upbit"                  # redite a J+1 ignoree ; autre lieu et annonce a J+20 gardes
    # identite des coins : fiches Aster et reponses de prix figees (formes reelles du 2026-10-06), sans reseau
    fiche = lambda sym, st, sub: {"symbol": sym, "contractType": "PERPETUAL", "underlyingType": "COIN", "symbolType": st, "underlyingSubType": sub}
    assert aster_crypto(fiche("HYPEUSDT", 0, [])) and aster_crypto(fiche("1000PEPEUSDT", 0, ["Meme"])) and aster_crypto(fiche("BTCUSDT", 0, ["Top"]))
    assert not aster_crypto(fiche("METAUSDT", 1, ["STOCK"])) and not aster_crypto(fiche("PAXGUSDT", 1, ["Commodities"]))       # l'action Meta, l'or
    assert not aster_crypto(fiche("POLYMARKETUSD1", 0, ["pre-launch", "STOCK", "AOS2", "USD1-RWA"])) and not aster_crypto(fiche("NEWUSDT", 0, ["DeFi"]))
    assert lot("1000PEPEUSDT") == lot("kPEPE") == lot("1000CHEEMS") == 1000 and lot("1MBABYDOGEUSDT") == 1e6 and lot("10000ELONUSDT") == 1e4
    assert lot("PEPE") == lot("1INCHUSDT") == lot("KAITOUSDT") == 1
    assert same_asset(1.0, 1.45) == (True, 1.45) and same_asset(1.0, 1.0 / 1.125)[0] and not same_asset(1.0, 1 / 1.6)[0] and not same_asset(2.0, 1.0)[0]   # XCN ; MANTRA Bithumb ; AI
    assert not same_asset(0.0128, 700.0)[0] and not same_asset(None, 1.0)[0] and same_asset(0.0, 1.0) == (False, None)     # META : Metadium (Upbit) contre l'action Meta
    assert same_asset(1.0e-5, 0.0101, lot("1000PEPEUSDT") / lot("PEPE"))[0] and not same_asset(1.0e-5, 0.0101)[0]          # cote par milliers : sans mult, x1010
    fake = {("https://api.upbit.com/v1/ticker", "KRW-CASHCAT,KRW-USDT"): [{"market": "KRW-CASHCAT", "trade_price": 204.0}, {"market": "KRW-USDT", "trade_price": 1400.0}],
            ("https://api.bithumb.com/v1/ticker", "KRW-BR,KRW-USDT"): [{"market": "KRW-BR", "trade_price": None}, {"market": "KRW-USDT", "trade_price": 1400.0}],
            ("https://api.bithumb.com/v1/ticker", "KRW-NOPE,KRW-USDT"): {"error": {"name": 404, "message": "Code not found"}},          # Bithumb : erreur en HTTP 200
            ("https://api.upbit.com/v1/ticker", "KRW-META,KRW-USDT"): [{"market": "KRW-META", "trade_price": 18.0}, {"market": "KRW-USDT", "trade_price": 1400.0}],
            (BINANCE_BOOK, "HYPEUSDT"): {"bidPrice": "92.5", "askPrice": "93.0"}, (BINANCE_BOOK, "XMRUSDT"): {"bidPrice": "0.0", "askPrice": "0.0"},
            (BINANCE_BOOK, "RAYUSDT"): {"bidPrice": "2.25", "askPrice": "2.5"}, (BINANCE_BOOK, "AIUSDT"): {"bidPrice": "0.29", "askPrice": "0.31"},
            (f"{COINBASE}/RAY-USD/ticker", None): {"bid": "2.0", "ask": "2.5"}, (f"{COINBASE}/AI-USD/ticker", None): {"bid": "0.039", "ask": "0.041"},
            (f"{COINBASE}/EURC-USDC/ticker", None): {"bid": "1.125", "ask": "1.375"}}

    class Resp:
        def __init__(self, body):
            self.status_code, self.body = (200, body) if body is not None else (404, {"message": "NotFound"})

        def json(self):
            return self.body
    real_get = requests.get
    requests.get = lambda url, params=None, **kw: Resp(fake.get((url, (params or {}).get("markets") or (params or {}).get("symbol"))))
    try:
        assert ref_price("upbit", "spot_krw", "cashcat") == 204.0 / 1400.0
        assert ref_price("bithumb", "spot_krw", "BR") is None and ref_price("bithumb", "spot_krw", "NOPE") is None     # marche ouvert sans echange ; ticker inconnu
        assert ref_price("binance", "spot", "HYPE") == 92.75 and ref_price("binance", "spot", "XMR") is None             # carnet vide : paire retiree
        assert ref_price("coinbase", "spot", "RAY") == 2.25 and ref_price("coinbase", "spot", "EURC") == 1.25 and ref_price("coinbase", "spot", "NOPE") is None
        assert ref_price("robinhood", "spot", "RAY") == 2.25 and ref_price("robinhood", "spot", "HYPE") == 92.75           # Coinbase d'abord, sinon Binance
        assert ref_price("robinhood", "spot", "AI") is None                         # Coinbase et Binance se contredisent (x7,5) : ticker ambigu
        assert ref_price("kraken", "spot", "RAY") is None
        assert not same_asset(ref_price("upbit", "spot_krw", "META"), 700.0)[0]      # collision : Metadium a ~0,013 $ contre METAUSDT a ~700 $
    finally:
        requests.get = real_get
    print("self-check OK")


if __name__ == "__main__":
    _selftest() if "--test" in sys.argv else main()
