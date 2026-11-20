"""
Scénarios de fraude (bank-realtime-lakehouse).

Dépose des transactions au format Kafka (même enveloppe que tx_generator.py)
dans /Volumes/bank/ops/landing/transactions/, pour qu'elles traversent
bronze -> silver -> bank.silver.transactions, puis la détection de fraude.

!! Vérifie que l'enveloppe correspond EXACTEMENT à celle de ton tx_generator.py
   (noms et types de key / value / topic / partition / offset / timestamp).
   Sinon la bronze verra une évolution de schéma. Adapte les lignes marquées (*).

Usage :
    import sys; sys.path.append("../generators")
    from fraud_scenarios import window_start, inject_all_scenarios, push_watermark
    ws = window_start()            # début de la fenêtre de 10 min en cours (UTC)
    inject_all_scenarios(ws)       # 4 cartes de test
    # ... plus tard, pour la partie "append" :
    push_watermark(ws)
"""

import json
import os
import uuid
from datetime import datetime, timedelta, timezone

LANDING = "/Volumes/bank/ops/landing/transactions"
_offset = [9_000_000]  # offsets fictifs, loin de ceux du générateur principal


def window_start(minutes: int = 10) -> datetime:
    """Début de la fenêtre tumbling de `minutes` qui contient l'heure actuelle (UTC)."""
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return now - timedelta(minutes=now.minute % minutes)


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds")


def inject(card_id: str, n: int, start: datetime, step_seconds: int = 30,
           amount: float = 120.0, account_id: str = "A001", tag: str = "lab3") -> str:
    msgs = []
    for i in range(n):
        et = start + timedelta(seconds=i * step_seconds)
        payload = {
            "txn_id": f"{tag}-{uuid.uuid4().hex[:12]}",
            "card_id": card_id,
            "pan": "4000000000000002",          # PAN fictif (carte de test)
            "account_id": account_id,
            "amount": amount,
            "currency": "EUR",
            "merchant": {"name": "Test Merchant", "mcc": 5732},
            "country": "FR",
            "channel": "ecommerce",
            "event_time": _iso(et),
        }
        _offset[0] += 1
        msgs.append({
            "key": card_id,
            "value": json.dumps(payload),        # (*) value = JSON en texte
            "topic": "card-transactions",
            "partition": 0,
            "offset": _offset[0],                # (*) entier
            "timestamp": _iso(et),               # (*) même type que dans tx_generator.py
        })
    os.makedirs(LANDING, exist_ok=True)
    path = f"{LANDING}/{tag}_{card_id}_{uuid.uuid4().hex[:6]}.json"
    with open(path, "w", encoding="utf-8") as f:
        for m in msgs:
            f.write(json.dumps(m) + "\n")
    print(f"{path} : {n} transactions, {start:%H:%M:%S} -> {(start + timedelta(seconds=(n - 1) * step_seconds)):%H:%M:%S} UTC")
    return path


def inject_all_scenarios(ws: datetime) -> None:
    """4 cartes, toutes dans la fenêtre [ws, ws+10 min) sauf CARD-SPLIT."""
    # 1. Vélocité : 6 transactions en 2 min 30 -> alerte n_tx >= 5
    inject("CARD-VELO", 6, ws + timedelta(minutes=1), step_seconds=30, amount=120.0)
    # 2. Montant : 2 x 1800 EUR -> alerte total > 3000
    inject("CARD-BIG", 2, ws + timedelta(minutes=2), step_seconds=60, amount=1800.0)
    # 3. Témoin : 3 petites transactions -> aucune alerte
    inject("CARD-OK", 3, ws + timedelta(minutes=2), step_seconds=60, amount=50.0)
    # 4. À cheval sur deux fenêtres : 6 tx de ws+7 à ws+12 (3 | 3) -> rate en tumbling
    inject("CARD-SPLIT", 6, ws + timedelta(minutes=7), step_seconds=60, amount=80.0)


def push_watermark(ws: datetime) -> None:
    """Une transaction à ws+25 min fait avancer le watermark (5 min) à ws+20 > fin de fenêtre."""
    inject("CARD-WM-PUSH", 1, ws + timedelta(minutes=25), amount=10.0)