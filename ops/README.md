# Exploitation

Le direct tourne sur un VPS europeen (Aster refuse les ordres venant des Etats-Unis, donc des serveurs GitHub Actions) :

- `/opt/listing-live/repo` : ce depot, mis a jour par `git pull` a chaque passage ; l'etat `live/` y est commite et pousse (cle de deploiement en ecriture limitee a ce depot).
- `/opt/listing-live/venv` : Python 3.12, dependances de `requirements.txt` installees avec `--no-deps`, puis `pip check`.
- `/opt/listing-live/env` (600) : cles d'agent Hyperliquid et Aster (trading seulement, aucun retrait) et HL_LIVE / ASTER_LIVE ; `telegram-bot`, `telegram-chat` (600) pour les alertes.
- `listing-live.timer` lance `listing-live.service` (`ops/run.sh`) toutes les 15 min ; `watchdog` (GitHub Actions) alerte sur Telegram si aucun etat n'a ete pousse depuis 2 h.

Arret d'urgence des entrees : creer `live/STOP` dans ce depot (les sorties continuent). Arret complet : `systemctl stop listing-live.timer`.
Une modification des dependances demande une reinstallation manuelle du venv.
