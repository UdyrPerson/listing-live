#!/bin/sh
# Un passage du direct sur le VPS (service listing-live, minuteur toutes les 15 min) : code a jour, self-checks, passage, etat pousse, alertes Telegram.
# Les cles ne sont exportees que pour le passage lui-meme (sous-shell), jamais pour git ni pour le reseau.
set -u
R=/opt/listing-live
PY=$R/venv/bin/python
cd $R/repo || exit 1
git pull -q --rebase origin main || echo "git pull en echec : passage sur le code deja present"
if $PY listing_feed.py --test && $PY listing_live.py --test && $PY listing_broker.py && $PY listing_broker_aster.py; then
  ( set -a; . $R/env; set +a; exec $PY listing_live.py ) || echo "passage en erreur"
else
  echo "SELF-CHECK EN ECHEC : passage annule, aucun ordre (voir journalctl -u listing-live)" > live/alerts.txt
fi
git add live/
git diff --cached --quiet || git commit -q -m "live: $(date -u +%FT%H:%MZ)"
for i in 1 2 3; do git pull -q --rebase origin main && git push -q origin main && break; sleep 5; done
$PY ops/notify.py live/alerts.txt
# tableau de bord : instantane vers Supabase si le fichier existe (URL, cle publiable, jeton ; 600, hors du depot) ; un echec n'affecte pas le passage
[ -r $R/supabase ] && ( set -a; . $R/supabase; set +a; exec timeout 120 $PY ops/publish.py ) || true
# homme mort : un ping healthchecks.io par passage, si le fichier d'URL existe (600, hors du depot) ; la periode (15 min) et la grace se reglent sur le site
[ -r $R/healthcheck ] && curl -fsS -m 10 -o /dev/null "$(cat $R/healthcheck)" || true
