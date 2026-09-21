# listing-live

Pilote experimental, en taille minuscule, d'une regle de trading sur les annonces de listing (perps Hyperliquid). Compte de test. Rien ici n'est un conseil.

- `listing_feed.py` : analyseurs d'annonces (API Upbit et Binance, titres de fils Telegram publics). Texte seul.
- `listing_live.py` : un passage = annonces -> signaux -> journal (`live/journal.csv`), avec le carnet releve a l'entree et a la sortie.
- `listing_broker.py` : ordres du pilote. Simulation par defaut ; les ordres ne partent que si la variable `HL_LIVE` vaut 1.
- `.github/workflows/listing-live.yml` : un passage par heure ; l'etat `live/` est versionne par la tache, chaque evenement ouvre une issue.

Securite : aucune cle dans ce depot. La cle d'agent (qui peut trader mais pas retirer) et l'adresse du compte sont des secrets GitHub Actions ;
les messages d'erreur masquent tout ce qui ressemble a une cle, une signature ou une adresse. Arret d'urgence : creer le fichier `live/STOP`.
