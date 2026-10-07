"""Envoie un fichier d'alertes au bot Telegram (fichiers telegram-bot et telegram-chat du dossier de deploiement, 600). Rien si le fichier est vide."""
import pathlib
import sys
import time
import urllib.parse
import urllib.request

R = pathlib.Path("/opt/listing-live")
f = pathlib.Path(sys.argv[1])
text = f.read_text(encoding="utf-8").strip() if f.exists() else ""
if text:
    token, chat = (R / "telegram-bot").read_text().strip(), (R / "telegram-chat").read_text().strip()
    data = urllib.parse.urlencode({"chat_id": chat, "text": f"listing-live {time.strftime('%d/%m %H:%M UTC', time.gmtime())}\n{text}"[:4000]}).encode()
    try:
        urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data, timeout=20)
    except Exception as e:                                          # le jeton n'apparait jamais : seulement le type d'erreur
        print(f"Telegram en echec : {type(e).__name__}")
