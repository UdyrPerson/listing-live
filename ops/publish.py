# Resume du direct pour le tableau de bord (artifact claude.ai) : comptes, PnL horaire, positions, signaux, trades clos, test sequentiel
# -> fonction push_snapshot d'une base Supabase. Lance par ops/run.sh apres chaque passage si /opt/listing-live/supabase existe
# (SUPABASE_URL, SUPABASE_KEY = cle publiable, SUPABASE_TOKEN = jeton dont la base ne garde que l'empreinte sha256).
# N'envoie que ce que live/ publie deja dans ce depot public : ni cle, ni adresse de compte.
import csv, json, os, pathlib, re, sys, time
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


def sizing(journal, eq, px):
    """Taille du pilote pour la page -> ({id: champs}, {exch: usage du compte}).
    Position reelle ouverte : w, part de la repartition et plafond qui a limite l'entree (lus dans r_note, ecrit par real_enter).
    Signal en attente : taille visee F x w x capitaux des comptes ou il se trade, parts prevues par allocate(), signaux pris dans l'ordre
    d'entree. Etat des comptes : short reel au prix du moment, moins les positions sorties avant l'entree du signal, plus les parts deja
    prevues ; marge estimee aux leviers du pilote. Estimation : les montants reels se fixent a l'entree, sur l'etat lu sur l'exchange."""
    E = {ex: rows[-1][1] for ex, rows in eq.items() if rows}
    by_id = {j["id"]: j for j in journal}                                # ligne jumelle : w de sa ligne principale, comme step()
    mk, out, held = 1 / ll.LEV_SHORT + 1 / ll.LEV_HEDGE, {}, []          # held : (exchange, short en $, sortie prevue)
    for j in journal:
        if j["r_size"] and j["r_net_usd"] == "" and j["exch"] in E:
            held.append((j["exch"], float(j["r_size"]) * (px.get(j["exch"], {}).get(j["coin"]) or float(j["r_px_in"])), int(j["exit_ts"] or 0)))
            part, lim = re.search(r"part de la repartition (\d+) \$", j["r_note"]), re.search(r"limite : ([^)]+)\)", j["r_note"])
            out[j["id"]] = {"w": round(ll.weight(by_id.get(j["twin"], j)), 3), "part": float(part.group(1)) if part else None, "limit": lim.group(1) if lim else None}
    usage = {ex: {"value": round(v, 2), "short": round(sum(z for e, z, _ in held if e == ex), 2), "n": sum(e == ex for e, _, _ in held)} for ex, v in E.items()}
    for ex, u in usage.items():
        u["pct"] = round(u["short"] / u["value"], 3)
    for j in sorted((j for j in journal if j["status"] == "signal" and not j["twin"]), key=lambda j: int(j["entry_ts"])):
        t = int(j["entry_ts"])
        group = [x for x in [j] + [x for x in journal if x["twin"] == j["id"] and x["status"] == "signal"]
                 if x["exch"] in E and x["market"] != "perp" and not x["pop"]]
        live = [(e, z) for e, z, out_ts in held if out_ts > t]
        acc = {x["exch"]: (E[x["exch"]], sum(z for e, z in live if e == x["exch"]), sum(z for e, z in live if e == x["exch"]) * mk, ll.MIN_LEG[x["exch"]])
               for x in group if sum(e == x["exch"] for e, _ in live) < ll.MAX_REAL}
        w = ll.weight(j)
        target = ll.F * w * sum(E[x["exch"]] for x in group)
        alloc = ll.allocate(target, acc) if acc else {}
        for x in group:
            out[x["id"]] = {"w": round(w, 3), "w_known": j["vol3d"] not in ("", "0", "0.0"), "target": round(target, 1), "plan": round(alloc.get(x["exch"], 0.0), 1)}
            if alloc.get(x["exch"]):
                held.append((x["exch"], alloc[x["exch"]], t + ll.HOLD_S))
    return out, usage


def trade_rows(journal, px, sizes=None):
    """Une entree par signal du journal, volet fictif (la regle, que juge le test sequentiel) et volet reel (ordres, latent ou realise), pour le journal
    des trades de la page. Latent reel = short + couverture au prix du moment, hors frais et funding."""
    out = []
    for j in journal:
        if j["status"] not in ("signal", "ouvert", "clos", "stoppe"):
            continue
        p, perp = px.get(j["exch"], {}), j["market"] == "perp"
        twin, t0 = bool(j["twin"]) or j["id"].endswith("-aster"), int(j["t_in"] or j["entry_ts"] or 0)
        x = {"id": j["id"], "coin": j["ticker"], "exch": j["exch"], "t_ann": int(float(j["t_ann"])), "twin": twin, "state": j["status"],
             "listing": LABEL.get((j["venue"], j["market"] or {"upbit": "spot_krw", "bithumb": "spot_krw"}.get(j["venue"], "spot")), j["venue"]),
             "test": not (twin or perp or j.get("pop") or j["id"] in ll.ERRATUM) and t0 >= ll.V11_FROM, "v": "v1.1" if t0 >= ll.V11_FROM else "v1",
             "out": j.get("pop", "") or ("listing de perp : fictif seulement" if perp else ""), "entry_ts": int(j["entry_ts"] or 0), "exit_ts": int(j["exit_ts"] or 0)}
        if j["t_in"]:
            x |= {"t_in": int(j["t_in"]), "px_in": float(j["px_in"] or j["r_px_in"] or 0)}
            if j["status"] == "ouvert" and j["coin"] in p and x["px_in"]:
                x["ret_bp"] = round((1 - p[j["coin"]] / x["px_in"]) * 1e4)
        if j["net_hedged"] != "":
            x |= {"t_out": int(j["t_out"]), "px_out": float(j["px_out"]), "short_bp": round(float(j["short_net"]) * 1e4),
                  "hedge_bp": round(float(j["hedge_net"]) * 1e4), "net_bp": round(float(j["net_hedged"]) * 1e4)}
        if j["r_size"]:
            size, p0, legs = float(j["r_size"]), float(j["r_px_in"]), json.loads(j["r_legs"] or "[]")
            r = {"usd": round(size * p0, 1), "open": j["r_net_usd"] == ""}
            if not r["open"]:
                r |= {"pnl": round(float(j["r_net_usd"]), 2), "pnl_bp": round(float(j["r_net_usd"]) / (size * p0) * 1e4), "early": bool(j.get("r_t_out")),
                      "t_out": int(j.get("r_t_out") or j["t_out"] or 0)}
            elif j["coin"] in p and all(c in p for c, _, _ in legs):
                r |= {"latent": round(size * (p0 - p[j["coin"]]) + sum(z * (p[c] - q) for c, z, q in legs), 2),
                      "hedge": round(sum(z * p[c] for c, z, _ in legs) / (size * p[j["coin"]]), 3)}
            x["real"] = r
        else:
            x["real"] = None
            x["why"] = ("hors test" if j.get("pop") else "listing de perp" if perp else "pas de place" if ll.capacity_refused(j["r_note"]) else "")
        if (sizes or {}).get(j["id"]):
            x["size"] = sizes[j["id"]]
        out.append(x)
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
            if j.get("r_t_out"):                                    # sortie reelle anticipee : la ligne ne suit plus que le fictif
                x["real"], x["listing"] = False, x["listing"] + time.strftime(" (reel solde le %d/%m %H:%M)", time.gmtime(int(j["r_t_out"])))
            open_.append(x)
        elif j["net_hedged"] != "":
            closed.append(tag(j) | {"t_in": int(j["t_in"]), "t_out": int(j["t_out"]), "px_in": float(j["px_in"]), "px_out": float(j["px_out"]),
                                    "stopped": j["status"] == "stoppe", "short_bp": round(float(j["short_net"]) * 1e4),
                                    "hedge_bp": round(float(j["hedge_net"]) * 1e4), "net_bp": round(float(j["net_hedged"]) * 1e4),
                                    "usd": round(float(j["r_net_usd"]), 2) if j["r_net_usd"] else None})
    n, s, verdict = ll.sprt_v11(journal)
    size, usage = sizing(journal, eq, px)
    accounts = {ex: {"ts": rows[-1][0], "value": rows[-1][1]} for ex, rows in eq.items() if rows}
    return {"ts": now, "accounts": accounts, "pnl": series(eq), "open": sorted(open_, key=lambda x: x["t_in"]),
            "signals": sorted(signals, key=lambda x: x["entry_ts"]), "closed": sorted(closed, key=lambda x: -x["t_out"]),
            "sprt": {"n": n, "s": round(s, 4), "verdict": verdict, "drift": ll.SPRT_DRIFT, "bound": ll.SPRT_BOUND}, "stop": stop,
            "trades": trade_rows(journal, px, size), "usage": usage}


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
    snap["pass_ts"] = int((ll.LIVE / "journal.csv").stat().st_mtime)   # dernier passage du bot : etat des trades ; ts = prix du moment (minuteur listing-publish)
    snap["px_ok"] = sorted(px)
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
    assert s["usage"]["hl"] == {"value": 149.0, "short": 16.0, "n": 1, "pct": 0.107} and s["usage"]["aster"]["short"] == 0
    sig = [j | {"id": "e", "ticker": "V", "coin": "V", "exch": "hl", "venue": "upbit", "market": "spot_krw", "t_ann": "100", "status": "signal",
                "entry_ts": "1500", "exit_ts": "9000", "vol3d": "0.16"},
           j | {"id": "e-aster", "ticker": "V", "coin": "VUSDT", "exch": "aster", "venue": "upbit", "market": "spot_krw", "t_ann": "100",
                "status": "signal", "entry_ts": "1500", "exit_ts": "9000", "twin": "e", "vol3d": "0.05"}]
    z, _ = sizing(rows + sig, {"hl": [(1, 149.0)], "aster": [(1, 153.53)]}, {"hl": {"X": 1.6}})
    assert z["e"]["w"] == 0.5 and z["e-aster"]["w"] == 0.5 and z["e"]["target"] == round(0.5 * 0.5 * (149 + 153.53), 1)   # w de la ligne principale
    assert abs(z["e"]["plan"] + z["e-aster"]["plan"] - z["e"]["target"]) < 0.2 and z["e-aster"]["plan"] > z["e"]["plan"]     # Aster, moins charge, recoit plus
    assert z["a"]["w"] == 1.0 and z["a"]["part"] is None
    tw = [x | {"status": "ouvert", "r_size": "10", "r_px_in": "1", "exit_ts": "9000"} for x in sig]
    z2, _ = sizing(tw, {"hl": [(1, 149.0)], "aster": [(1, 153.53)]}, {})
    assert z2["e-aster"]["w"] == z2["e"]["w"] == 0.5                                                  # position jumelle : w de la principale
    print("self-check ok")


if __name__ == "__main__":
    _selftest() if "--test" in sys.argv else main()
