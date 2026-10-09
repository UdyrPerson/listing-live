# Exploitation

Le direct tourne sur un VPS europeen (Aster refuse les ordres venant des Etats-Unis, donc des serveurs GitHub Actions) :

- `/opt/listing-live/repo` : ce depot, mis a jour par `git pull` a chaque passage ; l'etat `live/` y est commite et pousse (cle de deploiement en ecriture limitee a ce depot).
- `/opt/listing-live/venv` : Python 3.12, dependances de `requirements.txt` installees avec `--no-deps`, puis `pip check`.
- `/opt/listing-live/env` (600) : cles d'agent Hyperliquid et Aster (trading seulement, aucun retrait) et HL_LIVE / ASTER_LIVE ; `telegram-bot`, `telegram-chat` (600) pour les alertes.
- `listing-live.timer` lance `listing-live.service` (`ops/run.sh`) toutes les 15 min.
- Homme mort principal : chaque passage pingue healthchecks.io si `/opt/listing-live/healthcheck` (600, utilisateur `listing`) contient l'URL du check (periode 15 min, grace 20 min, reglees sur le site). Sans ce fichier, aucun ping.
- Tableau de bord : si `/opt/listing-live/supabase` (600, utilisateur `listing` : SUPABASE_URL, SUPABASE_KEY publiable, SUPABASE_TOKEN) existe, chaque passage envoie un instantane (`ops/publish.py`) a la fonction `push_snapshot` d'une base Supabase, qui ne garde que l'empreinte sha256 du jeton. Nouveau jeton : le generer, remplacer l'empreinte dans la fonction, reecrire le fichier.
- `watchdog` (GitHub Actions) reste en second filet : il alerte sur Telegram si aucun etat n'a ete pousse depuis 2 h, mais GitHub ne lance pas fidelement les crons horaires (2 passages programmes en ~18,6 h mesures le 2026-10-08) : il ne detecte un VPS muet qu'en 6 a 8 h.

Arret d'urgence des entrees : creer `live/STOP` dans ce depot (les sorties continuent). Arret complet : `systemctl stop listing-live.timer`.
Une modification des dependances demande une reinstallation manuelle du venv.
