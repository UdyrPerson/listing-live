# listing-live

Pilote experimental, en taille minuscule, d'une regle de trading sur les annonces de listing (perps Hyperliquid). Compte de test. Rien ici n'est un conseil.

- `listing_feed.py` : analyseurs d'annonces (API Upbit et Binance, titres de fils Telegram publics). Texte seul.
- `listing_live.py` : un passage = annonces -> signaux -> journal (`live/journal.csv`), avec le carnet releve a l'entree et a la sortie.
- `listing_broker.py` : ordres du pilote. Simulation par defaut ; les ordres ne partent que si la variable `HL_LIVE` vaut 1.
- `listing_broker_aster.py` : idem sur Aster (compte autonome) ; les ordres ne partent que si la variable `ASTER_LIVE` vaut 1.
  Un signal dont le perp existe sur les deux exchanges est pris sur les deux ; la taille se repartit pour equilibrer l'utilisation des deux comptes.
- `ops/` : exploitation sur un VPS europeen (passage toutes les 15 min, etat `live/` pousse ici, alertes Telegram) ; `.github/workflows/watchdog.yml` alerte si le VPS ne pousse plus d'etat.

Securite : aucune cle dans ce depot. La cle d'agent (qui peut trader mais pas retirer) et l'adresse du compte sont des secrets GitHub Actions ;
les messages d'erreur masquent tout ce qui ressemble a une cle, une signature ou une adresse. Arret d'urgence : creer le fichier `live/STOP` (cree aussi automatiquement par un arret dur ; le supprimer pour reprendre).
Dependances : toutes figees dans `requirements.txt`.
