"""Ordres REELS sur Aster (perps, fapi.asterdex.com) pour le pilote : meme interface que listing_broker.Broker, mais avec des SYMBOLES Aster
(QUSDT, 1000PEPEUSDT), pas des noms de coin. Charge par listing_live.py seulement si une cle est fournie.

    cle et compte : variables d'environnement ASTER_AGENT_KEY / ASTER_USER, sinon le fichier local gitignore listing_broker.KEY_FILE
                    (libelles "aster private key" et "rabby address").
    ASTER_LIVE=1  : les ordres partent. Sinon SIMULATION : rien n'est envoye, seules des lectures sont faites.

La cle est celle d'un agent Aster (API v3) : elle signe pour le compte `user`, sans droit de retrait. Elle n'est jamais affichee ni journalisee et tout
message d'erreur passe par why(). Le compte doit etre DEDIE a la strategie, en mode de position "one-way".
Piege ccxt : par defaut ccxt "aster" ajoute a chaque ordre perp un builder ccxt a 0,1 % du notionnel, et l'approuve de lui-meme (POST v3/approveBuilder)
au premier appel signe. builderFee=False coupe les deux.
"""
import os
import re
import time

from listing_broker import KEY_FILE, why

SLIPPAGE = 0.01                    # un ordre "au marche" est une limite IOC a 1 % du prix de reference : au-dela, pas d'execution plutot qu'un mauvais prix
MARGIN = 1.05                      # au-dessus du MIN_NOTIONAL : l'IOC part a 1 % du prix de reference, et le prix bouge entre le releve et l'envoi
FINAL = {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}


def credentials_aster():
    """-> (compte, cle) ou (None, None). Jamais a moitie. Ne jamais afficher le second element."""
    user, key = os.environ.get("ASTER_USER"), os.environ.get("ASTER_AGENT_KEY")
    if not (user and key) and KEY_FILE.exists():
        vals = {m.group(1).strip().lower(): m.group(2) for m in (re.match(r"\s*([A-Za-z _\-]+?)\s*[:=]\s*(\S+)", ln) for ln in KEY_FILE.read_text(encoding="utf-8").splitlines()) if m}
        user, key = vals.get("rabby address"), vals.get("aster private key")
    ok = user and key and re.fullmatch(r"(0x)?[0-9a-fA-F]{40}", user) and re.fullmatch(r"(0x)?[0-9a-fA-F]{64}", key)
    return (user, key) if ok else (None, None)


def on_step(x, step):
    """x au multiple de `step` le plus proche, sans residu flottant (0.30000000000000004 -> 0.3 ; 0.2999999999 -> 0.3, la ou ccxt tronquerait a 0.2)."""
    return round(round(x / step) * step, 12)


def round_size(usd, px, step, min_usd):
    """Taille pour ~usd de notionnel, au pas du marche, jamais sous min_usd."""
    n = max(1, round(usd / px / step))
    while n * step * px < min_usd:
        n += 1
    return round(n * step, 12)


class AsterBroker:
    exch = "aster"

    def __init__(self, user, key, live):
        import ccxt                                             # seulement ici : listing_live.py sans cle n'a besoin que de requests
        self.live = live
        self.x = x = ccxt.aster({"privateKey": key, "options": {"builderFee": False, "cachedWalletAddress": user.lower()}})
        try:
            signer = x.eth_get_address_from_private_key(key).lower()
            x.options["signerAddress"] = signer
            x.options["privateKeyHashForCachedWalletAddress"] = x.hash(x.encode(x.privateKey), "keccak", "hex")    # sinon ccxt remplace `user` par le signataire
            x.load_markets()
            self.sym = {m["id"]: s for s, m in x.markets.items() if m.get("swap")}      # QUSDT -> Q/USDT:USDT (le spot porte les memes identifiants)
            dual = x.fapiPrivateGetV3PositionSideDual()
            self.multi_assets = str(x.fapiPrivateGetV3MultiAssetsMargin().get("multiAssetsMargin")).lower() == "true"     # expose, jamais change
        except Exception as e:
            raise RuntimeError(f"connexion Aster en echec ({why(e)})") from None
        if str(dual.get("dualSidePosition")).lower() != "false":
            raise RuntimeError(f"compte Aster en mode de position hedge (dualSidePosition={dual.get('dualSidePosition')}) : le pilote exige le mode one-way")
        self.days_left = None                                   # jours avant l'expiration de l'agent : sans agent valide, plus d'entree NI de sortie
        try:
            mine = [a for a in x.fapiPrivateGetV3Agent() if str(a.get("agentAddress", "")).lower() == signer]
            self.days_left = (int(mine[0]["expired"]) / 1000 - time.time()) / 86400 if mine else None
        except Exception:
            pass

    def _m(self, symbol):
        return self.x.markets[self.sym[symbol]]

    def state(self):
        """-> (valeur du compte en $, {symbole: taille signee}, marge initiale bloquee en $), d'un seul GET v3/accountWithJoinMargin :
        totalMarginBalance = solde + PnL latent, TOUS collateraux valorises en $ apres decote ; totalInitialMargin = marge des positions ET des ordres
        ouverts ; positions[].positionAmt (signe, mode one-way). PAS v3/account : ses totaux ne comptent que l'USDT (releve du 2026-10-06, compte en
        mode multi-actifs avec 0,001 ETH : v3/account 0 $, accountWithJoinMargin 2,568 $ = 0,001 x 2 702 x ~0,95)."""
        try:
            a = self.x.fapiPrivateGetV3AccountWithJoinMargin()
        except Exception as e:
            raise RuntimeError(f"etat du compte Aster illisible ({why(e)})") from None
        pos = {p["symbol"]: float(p["positionAmt"]) for p in a.get("positions", []) if float(p.get("positionAmt") or 0) != 0}
        return float(a["totalMarginBalance"]), pos, float(a["totalInitialMargin"])

    def min_usd(self, symbol):
        """Notionnel minimum du marche (filtre MIN_NOTIONAL) avec la marge MARGIN : en dessous, l'ordre serait refuse."""
        return float(self._m(symbol)["limits"]["cost"]["min"]) * MARGIN

    def size_for(self, symbol, usd, px):
        """Taille au pas LOT_SIZE (celui des ordres limites, donc de nos IOC), jamais sous min_usd."""
        return round_size(usd, px, self._m(symbol)["precision"]["amount"], self.min_usd(symbol))

    def market(self, symbol, side, size, ref_px, reduce=False):
        """Limite IOC a ref x (1 +- SLIPPAGE), au pas de prix. -> (quantite executee, prix moyen), partielle comprise. Leve une erreur si rien n'est execute."""
        if not self.live:
            return size, ref_px
        s, m = self.sym[symbol], self._m(symbol)
        px = on_step(ref_px * (1 + SLIPPAGE if side == "buy" else 1 - SLIPPAGE), m["precision"]["price"])
        try:
            o = self.x.create_order(s, "limit", side, on_step(size, m["precision"]["amount"]), px, {"timeInForce": "IOC"} | ({"reduceOnly": "true"} if reduce else {}))["info"]
        except Exception as e:
            raise RuntimeError(f"ordre refuse ({why(e)})") from None
        o = self._final(s, o)
        filled = float(o.get("executedQty") or 0)
        avg = float(o.get("avgPrice") or 0) or (float(o.get("cumQuote") or 0) / filled if filled else 0.0)
        if filled <= 0 or avg <= 0:
            raise RuntimeError(f"ordre non execute (statut {o.get('status')} : glissement > 1 % ou carnet vide)")
        return filled, avg

    def _final(self, s, o, tries=20):
        """Relit l'ordre jusqu'a son statut final : la reponse immediate peut dire NEW, executedQty 0 (accuse de reception a la Binance).
        Un IOC encore ouvert apres ~10 s est annule puis relu une derniere fois."""
        oid = str(o.get("orderId"))
        for i in range(tries + 1):
            if o.get("status") in FINAL:
                return o
            if i == tries:
                try:
                    self.x.cancel_order(oid, s)
                except Exception:
                    pass
            time.sleep(0.5)
            try:
                o = self.x.fetch_order(oid, s)["info"]
            except Exception as e:
                if i == tries:
                    raise RuntimeError(f"ordre {oid} envoye, etat final illisible ({why(e)}) : verifier le compte") from None
        return o

    def stop(self, symbol, size, trigger):
        """Stop de protection : STOP_MARKET d'achat reduce-only declenche sur le prix MARK. Il repose sur l'exchange, pas sur ce programme."""
        if not self.live:
            return "simulation"
        s, m = self.sym[symbol], self._m(symbol)
        try:
            o = self.x.create_order(s, "STOP_MARKET", "buy", on_step(size, m["precision"]["amount"]), None,         # declencheur au pas de prix, sinon refus
                                    {"stopPrice": on_step(trigger, m["precision"]["price"]), "reduceOnly": "true", "workingType": "MARK_PRICE"})
        except Exception as e:
            raise RuntimeError(f"stop refuse ({why(e)})") from None
        return str(o.get("id") or "pose")

    def cancel_all(self, symbol):
        if self.live:
            try:
                self.x.cancel_all_orders(self.sym[symbol])
            except Exception as e:
                raise RuntimeError(f"annulation refusee ({why(e)})") from None

    def leverage(self, symbol, lev):
        """Marge CROISEE puis levier = min(lev, levier maximal des paliers du symbole). En croisee il ne fixe que la marge initiale bloquee."""
        if not self.live:
            return
        import ccxt
        s = self.sym[symbol]
        try:
            try:
                self.x.set_margin_mode("cross", s)
            except Exception as e:
                if not (isinstance(e, ccxt.NoChange) or "no need to change margin type" in str(e).lower()):
                    raise
            self.x.set_leverage(min(int(lev), self.max_leverage(symbol)), s)
        except Exception as e:
            raise RuntimeError(f"levier refuse ({why(e)})") from None

    def max_leverage(self, symbol):
        """Levier maximal du symbole = le plus haut initialLeverage de ses paliers (GET v3/leverageBracket, lecture)."""
        b = self.x.fapiPrivateGetV3LeverageBracket({"symbol": symbol})
        return max(int(float(k["initialLeverage"])) for r in (b if isinstance(b, list) else [b]) if r.get("symbol") == symbol for k in r["brackets"])


def connect_aster():
    """-> AsterBroker, ou None s'il n'y a pas de cle. Reel seulement si ASTER_LIVE=1."""
    user, key = credentials_aster()
    return AsterBroker(user, key, os.environ.get("ASTER_LIVE") == "1") if key else None


def _selftest():
    assert on_step(0.30000000000000004, 0.1) == 0.3 and on_step(0.2999999999, 0.1) == 0.3 and on_step(5072.4, 1) == 5072
    assert on_step(2673.1234 * 1.01, 0.01) == 2699.85 and on_step(0.05123 * 0.99, 0.00001) == 0.05072                  # IOC d'achat / de vente au pas de prix
    assert round_size(12, 4000.0, 0.001, 5.25) == 0.003 and round_size(12, 0.1234, 1, 5.25) == 97                      # 12 $ d'ETH, 12 $ d'un petit coin
    assert round_size(3, 1.0, 1, 5.25) == 6 and round_size(12, 95000.0, 0.001, 5.25) == 0.001                         # monte au minimum ; pas trop gros : 1 pas
    assert all(round_size(u, p, st, 5.25) * p >= 5.25 for u, p, st in [(1, 0.37, 1), (5, 2.2, 0.1), (12, 61000, 0.001)])
    secret = "0x" + "ab" * 32
    assert "<masque>" in why(RuntimeError(f"signature {secret} refusee")) and "abab" not in why(RuntimeError(secret))

    saved = {k: os.environ.pop(k, None) for k in ("ASTER_USER", "ASTER_AGENT_KEY")}
    try:
        assert credentials_aster() == (None, None) or all(credentials_aster())                       # fichier local : compte ET cle, ou rien
        os.environ |= {"ASTER_USER": "0x" + "12" * 20, "ASTER_AGENT_KEY": "pas-une-cle"}
        assert credentials_aster() == (None, None)                                                   # cle malformee : rien, pas le compte seul
        os.environ["ASTER_AGENT_KEY"] = secret
        assert credentials_aster() == ("0x" + "12" * 20, secret)
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)

    class X:                                                    # faux ccxt : accuse NEW puis execution partielle, sans reseau
        markets = {"Q/USDT:USDT": {"precision": {"amount": 1, "price": 0.0001}, "limits": {"cost": {"min": 5}}}}
        sent, reads = [], []

        def create_order(self, *a):
            self.sent.append(a)
            return {"info": {"orderId": 7, "status": "NEW", "executedQty": "0", "avgPrice": "0"}}

        def fetch_order(self, oid, s):
            self.reads.append(oid)
            return {"info": {"orderId": 7, "status": "EXPIRED", "executedQty": self.q, "avgPrice": "0", "cumQuote": "40.2"}}

    b = AsterBroker.__new__(AsterBroker)
    b.live, b.x, b.sym = True, X(), {"QUSDT": "Q/USDT:USDT"}
    b.x.q = "400"
    assert b.market("QUSDT", "buy", 600.0000000001, 0.1, reduce=True) == (400.0, 40.2 / 400)       # partiel : la quantite reellement executee
    assert b.x.sent[-1] == ("Q/USDT:USDT", "limit", "buy", 600, 0.101, {"timeInForce": "IOC", "reduceOnly": "true"}) and b.x.reads == ["7"]
    b.market("QUSDT", "sell", 600, 0.1)
    assert b.x.sent[-1][4] == 0.099 and b.x.sent[-1][5] == {"timeInForce": "IOC"}
    b.x.q = "0"
    try:
        b.market("QUSDT", "sell", 600, 0.1)
        raise AssertionError("un IOC sans execution doit lever une erreur")
    except RuntimeError as e:
        assert "non execute" in str(e)
    assert b.size_for("QUSDT", 12, 0.1234) == 97 and b.min_usd("QUSDT") == 5.25
    b.live = False
    assert b.market("QUSDT", "sell", 97, 0.1234) == (97, 0.1234) and b.stop("QUSDT", 97, 0.13) == "simulation"
    print("self-check OK")


if __name__ == "__main__":
    _selftest()
