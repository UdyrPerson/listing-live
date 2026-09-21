"""Ordres REELS sur Hyperliquid pour le pilote en taille minuscule de la regle listing v1. Charge par listing_live.py seulement si une cle est fournie.

    cle et compte : variables d'environnement HL_AGENT_KEY / HL_ACCOUNT (secrets GitHub), sinon le fichier local gitignore KEY_FILE.
    HL_LIVE=1     : les ordres partent. Sinon SIMULATION : tout est construit, controle et journalise, rien n'est envoye.

La cle est une cle d'agent Hyperliquid : elle signe des ordres, elle ne peut pas retirer de fonds. Elle n'est jamais affichee ni journalisee, et les
messages d'erreur de signature ne sont pas recopies (seulement leur type). Le compte doit etre DEDIE a la strategie : le rapprochement suppose que
toute position qui s'y trouve vient d'ici.
"""
import math
import os
import pathlib
import re
import time

import requests

KEY_FILE = pathlib.Path("docs/superpowers/API-HL-Key/private")
INFO = "https://api.hyperliquid.xyz/info"
SLIPPAGE = "0.01"                  # un ordre "au marche" est une limite IOC a 1 % du prix de reference : au-dela, pas d'execution plutot qu'un mauvais prix
LEVERAGE = 3                       # levier du compte (marge croisee), jamais de marge isolee sur un short (framework, section 4)
MIN_USD = 10.5                     # Hyperliquid refuse un ordre de moins de 10 $


def credentials():
    """-> (compte, cle) ou (None, None). Ne jamais afficher le second element."""
    acct, key = os.environ.get("HL_ACCOUNT"), os.environ.get("HL_AGENT_KEY")
    if not (acct and key) and KEY_FILE.exists():
        vals = {m.group(1).strip().lower(): m.group(2) for m in (re.match(r"\s*([A-Za-z _\-]+?)\s*[:=]\s*(\S+)", ln) for ln in KEY_FILE.read_text(encoding="utf-8").splitlines()) if m}
        key = next((v for k, v in vals.items() if "key" in k and "aster" not in k and len(v.removeprefix("0x")) == 64), None)      # le meme fichier porte aussi la cle Aster
        acct = next((v for k, v in vals.items() if "rabby" in k or "account" in k or "master" in k), None)
    return (acct, key) if acct and key else (None, None)


def why(e):
    """Message d'erreur utile au diagnostic, sans rien qui ressemble a une cle, a une signature ni a une adresse de compte : les alertes sont publiques."""
    return f"{type(e).__name__} : " + re.sub(r"(0x)?[0-9a-fA-F]{40,}", "<masque>", str(e))[:220]


def round_size(usd, px, step):
    """Taille pour ~usd de notionnel, arrondie au pas du marche, jamais sous le minimum de l'exchange."""
    n = max(1, round(usd / px / step))
    while n * step * px < MIN_USD:
        n += 1
    return round(n * step, 10)


class Broker:
    def __init__(self, account, key, live):
        import ccxt                                             # seulement ici : listing_live.py sans cle n'a besoin que de requests
        self.account, self.live = account, live
        self.x = ccxt.hyperliquid({"walletAddress": account, "privateKey": key})
        self.x.load_markets()
        self.sym = {m["info"]["name"]: s for s, m in self.x.markets.items() if m.get("swap") and ":" not in str(m["info"].get("name"))}     # hors marches HIP-3
        self.days_left = None                                   # jours avant l'expiration de la cle d'agent : sans cle valide, plus d'entree NI de sortie
        try:
            agent = self.x.eth_get_address_from_private_key(key).lower()
            mine = [a for a in requests.post(INFO, json={"type": "extraAgents", "user": account}, timeout=30).json() if a["address"].lower() == agent]
            self.days_left = (mine[0]["validUntil"] / 1000 - time.time()) / 86400 if mine else None
        except Exception:
            pass

    def state(self):
        """-> (valeur du compte en $, {coin: taille signee}). En mode "unified account" la marge est le USDC du cote spot."""
        ch = requests.post(INFO, json={"type": "clearinghouseState", "user": self.account}, timeout=30).json()
        spot = requests.post(INFO, json={"type": "spotClearinghouseState", "user": self.account}, timeout=30).json().get("balances", [])
        usdc = sum(float(b["total"]) for b in spot if b["coin"] == "USDC")
        upnl = sum(float(p["position"]["unrealizedPnl"]) for p in ch["assetPositions"])
        value = max(float(ch["marginSummary"]["accountValue"]), usdc + upnl)
        return value, {p["position"]["coin"]: float(p["position"]["szi"]) for p in ch["assetPositions"] if float(p["position"]["szi"]) != 0}

    def size_for(self, coin, usd, px):
        return round_size(usd, px, self.x.markets[self.sym[coin]]["precision"]["amount"])

    def market(self, coin, side, size, ref_px, reduce=False):
        """Ordre IOC plafonne a SLIPPAGE du prix de reference. -> (taille executee, prix moyen). Leve une erreur si rien n'est execute."""
        if not self.live:
            return size, ref_px
        try:
            o = self.x.create_order(self.sym[coin], "market", side, size, ref_px, {"slippage": SLIPPAGE, "reduceOnly": reduce})
        except Exception as e:
            raise RuntimeError(f"ordre refuse ({why(e)})") from None
        filled, avg = float(o.get("filled") or 0), float(o.get("average") or 0)
        if filled <= 0 or avg <= 0:
            raise RuntimeError("ordre non execute (glissement > 1 % ou carnet vide)")
        return filled, avg

    def stop(self, coin, size, trigger):
        """Stop de protection : achat au marche reduce-only declenche a `trigger`. Il repose sur l'exchange, pas sur ce programme."""
        if not self.live:
            return "simulation"
        try:
            o = self.x.create_order(self.sym[coin], "market", "buy", size, trigger, {"stopLossPrice": trigger, "reduceOnly": True, "slippage": "0.05"})
        except Exception as e:
            raise RuntimeError(f"stop refuse ({why(e)})") from None
        return str(o.get("id") or "pose")

    def cancel_all(self, coin):
        if self.live:
            for o in self.x.fetch_open_orders(self.sym[coin]):
                self.x.cancel_order(o["id"], self.sym[coin])

    def leverage(self, coin):
        if self.live:
            lev = min(LEVERAGE, int(self.x.markets[self.sym[coin]]["info"].get("maxLeverage") or LEVERAGE))
            self.x.set_leverage(lev, self.sym[coin], {"marginMode": "cross"})


def connect():
    """-> Broker, ou None s'il n'y a pas de cle (suivi fictif seul)."""
    acct, key = credentials()
    return Broker(acct, key, os.environ.get("HL_LIVE") == "1") if key else None


def _selftest():
    assert round_size(22, 0.97, 0.1) == 22.7 and round_size(11, 2673.0, 0.0001) == 0.0041            # 22 $ de SUI, 11 $ d'ETH
    assert round_size(11, 95000.0, 0.001) == 0.001 * math.ceil(MIN_USD / 95000.0 / 0.001)            # pas trop gros : on monte jusqu'au minimum de 10 $
    assert round_size(11, 95000.0, 0.001) * 95000.0 >= MIN_USD
    os.environ.pop("HL_AGENT_KEY", None)
    assert credentials() == (None, None) or all(credentials())                                       # jamais a moitie : compte ET cle, ou rien
    print("self-check OK")


if __name__ == "__main__":
    _selftest()
