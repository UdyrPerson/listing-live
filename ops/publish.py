# Resume du direct pour le tableau de bord (artifact claude.ai) : comptes, PnL horaire, positions, signaux, trades clos, test sequentiel
# -> fonction push_snapshot d'une base Supabase. Lance par ops/run.sh apres chaque passage si /opt/listing-live/supabase existe
# (SUPABASE_URL, SUPABASE_KEY = cle publiable, SUPABASE_TOKEN = jeton dont la base ne garde que l'empreinte sha256).
# N'envoie que ce que live/ publie deja dans ce depot public : ni cle, ni adresse de compte.
import csv, json, os, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import requests
import listing_live as ll

# ponytail: bases fixes ; un depot ou un transfert entre comptes les fausse -> les mettre a jour ici
BASE = {"hl": 150.0, "aster": 152.53}      # apports nets : Hyperliquid 150 $ ; Aster 150 USDC + ~2,5 $ d'ETH (premier releve reel)
HL_FROM = 1790960000                       # releves Hyperliquid fiables apres le correctif du 02/10 (valeur sans double comptage) ; avant, la page garde son historique
LABEL = {("upbit", "spot_krw"): "Upbit KRW", ("bithumb", "spot_krw"): "Bithumb KRW", ("coinbase", "spot"): "spot Coinbase",
         ("binance", "spot"): "spot Binance", ("robinhood", "spot"): "Robinhood"}


def equity(name):
    """live/<name> -> [(ts, valeur)] des releves reels (Aster : les premiers releves, avant le depot, sont ecartes)."""
    path = ll.LIVE / name
    rows = list(csv.reader(open(path, encoding="utf-8"))) if path.exists() else []
    return [(int(r[0]), float(r[1])) for r in rows if len(r) >= 3 and r[2] == "reel" and float(r[1]) > 100]


def series(eq):
    """{exch: [(ts, valeur)]} -> [[ts, pnl hl, pnl aster, total]] : dernier releve de chaque heure, valeur portee en avant ; Aster vaut 0 avant son premier releve."""
    last = {ex: {t // 3600: (t, v - BASE[ex]) for t, v in rows} for ex, rows in eq.items()}
    out, cur = [], {ex: 0.0 for ex in eq}
    pts = sorted((t, ex, p) for ex, d in last.items() for t, p in d.values())
    for t, ex, p in pts:
        cur[ex] = p
        if out and out[-1][0] == t:
            out.pop()
        out.append([t] + [round(cur[ex], 2) for ex in ("hl", "aster")])
    for r in out:
        r.append(round(r[1] + r[2], 2))
    return out


def build(journal, eq, px, now, stop=False):
    """Instantane pour la page. px = {exch: {coin: prix}} ; une position dont un prix manque garde ses champs de marche a None."""
    def tag(j):
        return {"coin": j["ticker"], "exch": j["exch"], "real": j["real"] == "reel", "twin": bool(j["twin"]) or j["id"].endswith("-aster"),
                "listing": LABEL.get((j["venue"], j["market"] or {"upbit": "spot_krw", "bithumb": "spot_krw"}.get(j["venue"], "spot")), j["venue"]),
                "t_ann": int(float(j["t_ann"]))}
    open_, signals, closed = [], [], []
    for j in journal:
        if j["status"] == "signal":
            signals.append(tag(j) | {"entry_ts": int(j["entry_ts"])})
        elif j["status"] == "ouvert" and j["t_in"]:
            p = px.get(j["exch"], {})
            x = tag(j) | {"t_in": int(j["t_in"]), "exit_ts": int(j["exit_ts"]), "ret_bp": None, "hedge": None, "usd": None}
            if x["real"] and j["r_size"]:
                size, p0, legs = float(j["r_size"]), float(j["r_px_in"]), json.loads(j["r_legs"] or "[]")
                x |= {"px_in": p0, "usd": round(size * p0, 1), "legs": [c for c, _, _ in legs]}
                if j["coin"] in p and all(c in p for c, _, _ in legs):
                    x["hedge"] = round(sum(z * p[c] for c, z, _ in legs) / (size * p[j["coin"]]), 3)
            else:
                x["px_in"] = float(j["px_in"])
            if j["coin"] in p:
                x["ret_bp"] = round((1 - p[j["coin"]] / x["px_in"]) * 1e4)
            open_.append(x)
        elif j["net_hedged"] != "":
            closed.append(tag(j) | {"t_in": int(j["t_in"]), "t_out": int(j["t_out"]), "px_in": float(j["px_in"]), "px_out": float(j["px_out"]),
                                    "stopped": j["status"] == "stoppe", "short_bp": round(float(j["short_net"]) * 1e4),
                                    "hedge_bp": round(float(j["hedge_net"]) * 1e4), "net_bp": round(float(j["net_hedged"]) * 1e4),
                                    "usd": round(float(j["r_net_usd"]), 2) if j["r_net_usd"] else None})
    n, s, verdict = ll.sprt_v11(journal)
    accounts = {ex: {"ts": rows[-1][0], "value": rows[-1][1]} for ex, rows in eq.items() if rows}
    return {"ts": now, "accounts": accounts, "pnl": series(eq), "open": sorted(open_, key=lambda x: x["t_in"]),
            "signals": sorted(signals, key=lambda x: x["entry_ts"]), "closed": sorted(closed, key=lambda x: -x["t_out"]),
            "sprt": {"n": n, "s": round(s, 4), "verdict": verdict, "drift": ll.SPRT_DRIFT, "bound": ll.SPRT_BOUND}, "stop": stop}


def main():
    with open(ll.LIVE / "journal.csv", encoding="utf-8", newline="") as f:
        journal = list(csv.DictReader(f))
    eq = {"hl": [x for x in equity("equity.csv") if x[0] >= HL_FROM], "aster": equity("equity_aster.csv")}
    px = {}
    try:
        px["hl"] = {c: float(v) for c, v in ll.info({"type": "allMids"}).items()}
    except Exception as e:
        print("prix Hyperliquid indisponibles :", type(e).__name__)
    try:
        px["aster"] = {c: float(v["midPx"]) for c, v in ll.aster_prices().items()}
    except Exception as e:
        print("prix Aster indisponibles :", type(e).__name__)
    snap = build(journal, eq, px, int(time.time()), (ll.LIVE / "STOP").exists())
    url, key, token = (os.environ.get(k) for k in ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_TOKEN"))
    if not (url and key and token):
        print("pas de Supabase configure : instantane non envoye ;", len(json.dumps(snap)), "octets,", len(snap["open"]), "positions,", len(snap["pnl"]), "points de PnL")
        return
    r = requests.post(url.rstrip("/") + "/rest/v1/rpc/push_snapshot", json={"token": token, "snap": snap},
                      headers={"apikey": key, "Content-Type": "application/json"}, timeout=30)
    print("tableau de bord :", r.status_code, "" if r.ok else r.text[:200])


def _selftest():
    j = {c: "" for c in ll.J_COLS + ll.R_COLS}
    rows = [j | {"id": "a", "ticker": "X", "coin": "X", "exch": "hl", "venue": "upbit", "t_ann": "100", "status": "ouvert", "t_in": "1000", "exit_ts": "2000",
                 "real": "reel", "r_size": "10", "r_px_in": "2.0", "r_legs": json.dumps([["ETH", 0.005, 2000.0], ["SOL", 0.05, 100.0]])},
            j | {"id": "b-aster", "ticker": "Y", "coin": "YUSDT", "exch": "aster", "venue": "bithumb", "market": "spot_krw", "t_ann": "100", "status": "ouvert",
                 "t_in": "1000", "exit_ts": "2000", "px_in": "1.0", "twin": ""},
            j | {"id": "c", "ticker": "Z", "coin": "Z", "exch": "hl", "venue": "coinbase", "market": "spot", "t_ann": "50", "status": "clos", "t_in": "60",
                 "t_out": "90", "px_in": "1", "px_out": "0.9", "short_net": "0.1", "hedge_net": "-0.02", "net_hedged": "0.08", "real": "reel", "r_net_usd": "1.234"},
            j | {"id": "d", "ticker": "W", "coin": "", "exch": "", "venue": "upbit", "t_ann": "100", "status": "ineligible"}]
    s = build(rows, {"hl": [(3600, 151.0), (3700, 152.0), (7300, 149.0)], "aster": [(7200, 153.53)]},
              {"hl": {"X": 1.6, "ETH": 2000.0, "SOL": 100.0}, "aster": {"YUSDT": 1.1}}, 9999)
    x, y = s["open"]
    assert x["ret_bp"] == 2000 and x["hedge"] == round(15 / 16, 3) and x["usd"] == 20.0 and x["listing"] == "Upbit KRW"
    assert y["ret_bp"] == -1000 and y["hedge"] is None and y["twin"] and not y["real"] and y["listing"] == "Bithumb KRW"
    assert s["closed"][0]["net_bp"] == 800 and s["closed"][0]["usd"] == 1.23 and s["signals"] == []
    assert s["pnl"] == [[3700, 2.0, 0.0, 2.0], [7200, 2.0, 1.0, 3.0], [7300, -1.0, 1.0, 0.0]], s["pnl"]
    assert s["accounts"]["hl"]["value"] == 149.0 and s["sprt"]["verdict"] == "en cours"
    print("self-check ok")


if __name__ == "__main__":
    _selftest() if "--test" in sys.argv else main()
