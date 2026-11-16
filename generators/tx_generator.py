"""Générateur de transactions carte au format Kafka.

Deux sorties possibles :
- "files" (défaut) : fichiers JSON déposés dans un volume, une ligne par message Kafka
  {key, value, topic, partition, offset, timestamp}. Auto Loader les lit en bronze.
- "kafka" : envoi réel vers un topic (Confluent Cloud), depuis ta machine locale.

Le payload (champ `value`, JSON en texte) contient :
txn_id, card_id, pan (fictif), account_id, amount, currency, merchant {name, mcc},
country, channel, event_time.

Anomalies injectées volontairement :
- invalid_ratio : txn_id nul ou montant <= 0       -> expectations DROP ROW / quarantaine
- dup_ratio     : même payload renvoyé (redelivery) -> déduplication avec watermark
- late_ratio    : event_time décalé de 30 à 120 min -> watermark / MV qui se corrige
- devise "XXX"  : environ 5 % des lignes            -> expectation en mode warn
- burst_card    : rafale de transactions sur une carte -> alerte de fraude 
- with_device_id: champ en plus DANS le payload     -> aucun impact sur le schéma (VARIANT)
- with_headers  : colonne `headers` en plus dans l'enveloppe Kafka -> évolution de schéma Auto Loader

Utilisation dans un notebook Databricks :
    import sys; sys.path.insert(0, "/Workspace/Users/<toi>/bank-realtime-lakehouse")
    from generators.tx_generator import generate
    generate(n=1000, files=2)

Utilisation en local (Kafka) :
    export KAFKA_BOOTSTRAP=... KAFKA_API_KEY=... KAFKA_API_SECRET=...
    python generators/tx_generator.py --sink kafka --n 500
"""
from __future__ import annotations

import argparse
import json
import os
import random
import uuid
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

LANDING = "/Volumes/bank/ops/landing/transactions"
STATE_DIR = "/Volumes/bank/ops/landing/_generator_state"  # hors du dossier lu par Auto Loader
TOPIC = "card-transactions"
N_PARTITIONS = 3

MERCHANTS = [
    ("Carrefour", 5411), ("Monoprix", 5411), ("SNCF", 4112), ("Air France", 3007),
    ("Amazon", 5999), ("Uber", 4121), ("Fnac", 5732), ("TotalEnergies", 5541),
    ("Betclic", 7995), ("Booking.com", 4722),
]
COUNTRIES = ["FR"] * 6 + ["DE", "ES", "IT", "BE"]
CHANNELS = ["pos", "ecommerce", "contactless", "atm"]
CURRENCIES = ["EUR"] * 17 + ["USD", "GBP", "XXX"]


def _card_pool(n_cards: int) -> list[dict]:
    """Pool de cartes stable d'un run à l'autre (graine fixe) : mêmes card_id, PAN et comptes."""
    rng = random.Random(42)
    cards = []
    for i in range(n_cards):
        cards.append({
            "card_id": f"C{i:05d}",
            "pan": "4" + "".join(str(rng.randint(0, 9)) for _ in range(15)),
            "account_id": f"A{i // 2:05d}",  # 2 cartes par compte
            "country": rng.choice(COUNTRIES),
        })
    return cards


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds")


def _payload(card: dict, now: datetime, rng: random.Random, late_ratio: float,
             with_device_id: bool) -> tuple[dict, bool]:
    event_time = now - timedelta(seconds=rng.randint(0, 300))
    late = rng.random() < late_ratio
    if late:
        event_time -= timedelta(minutes=rng.randint(30, 120))
    name, mcc = rng.choice(MERCHANTS)
    p = {
        "txn_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "pan": card["pan"],
        "account_id": card["account_id"],
        "amount": round(rng.lognormvariate(3.5, 1.0), 2),  # médiane ~33 €
        "currency": rng.choice(CURRENCIES),
        "merchant": {"name": name, "mcc": mcc},
        "country": card["country"] if rng.random() < 0.9 else rng.choice(COUNTRIES),
        "channel": rng.choice(CHANNELS),
        "event_time": _iso(event_time),
    }
    if with_device_id:
        p["device_id"] = f"D{rng.randint(1, 9999):04d}"
    return p, late


def _corrupt(p: dict, rng: random.Random) -> None:
    if rng.random() < 0.5:
        p["txn_id"] = None
    else:
        p["amount"] = rng.choice([0.0, -round(rng.uniform(1, 200), 2)])


def _burst(card: dict, now: datetime, rng: random.Random, size: int,
           with_device_id: bool) -> list[dict]:
    """`size` transactions e-commerce élevées sur une même carte en moins de 3 minutes."""
    out = []
    for i in range(size):
        p, _ = _payload(card, now, rng, 0.0, with_device_id)
        p["event_time"] = _iso(now - timedelta(seconds=30 * i))
        p["amount"] = round(rng.uniform(400, 900), 2)
        p["currency"] = "EUR"
        p["channel"] = "ecommerce"
        out.append(p)
    return out


def _load_offsets(state_path: Path) -> dict[int, int]:
    if state_path.exists():
        return {int(k): v for k, v in json.loads(state_path.read_text()).items()}
    return {p: 0 for p in range(N_PARTITIONS)}


def _write_files(payloads: list[dict], files: int, landing: str, state_dir: str,
                 now: datetime, with_headers: bool = False) -> list[str]:
    landing_path = Path(landing)
    landing_path.mkdir(parents=True, exist_ok=True)
    state_path = Path(state_dir) / "offsets.json"
    offsets = _load_offsets(state_path)

    stamp = now.strftime("%Y%m%dT%H%M%S%f")
    written = []
    for i in range(files):
        lines = []
        for p in payloads[i::files]:
            key = p["card_id"]
            part = zlib.crc32(key.encode()) % N_PARTITIONS  # même carte -> même partition
            msg = {
                "key": key,
                "value": json.dumps(p),
                "topic": TOPIC,
                "partition": part,
                "offset": offsets[part],
                "timestamp": _iso(datetime.now(timezone.utc)),
            }
            if with_headers:
                msg["headers"] = {"source": "core-cards", "schema_version": "2"}
            lines.append(json.dumps(msg))
            offsets[part] += 1
        path = landing_path / f"tx_{stamp}_{i:03d}.json"
        path.write_text("\n".join(lines) + "\n")  # une seule écriture : pas de fichier partiel
        written.append(str(path))

    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(offsets))
    return written


def _send_kafka(payloads: list[dict], topic: str) -> None:
    from confluent_kafka import Producer  # pip install confluent-kafka

    producer = Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
    })
    for p in payloads:
        producer.produce(topic, key=p["card_id"], value=json.dumps(p))
        producer.poll(0)
    producer.flush()


def generate(n: int = 1000, files: int = 1, landing: str = LANDING, state_dir: str = STATE_DIR,
             invalid_ratio: float = 0.02, dup_ratio: float = 0.01, late_ratio: float = 0.03,
             with_device_id: bool = False, with_headers: bool = False, burst_card: str | None = None, burst_size: int = 6,
             sink: str = "files", topic: str = TOPIC, n_cards: int = 500,
             seed: int | None = None) -> dict:
    """Génère un lot et renvoie un résumé, pour vérifier tes `count` en bronze et en silver."""
    rng = random.Random(seed)
    cards = _card_pool(n_cards)
    now = datetime.now(timezone.utc)

    payloads, n_invalid, n_late = [], 0, 0
    for _ in range(n):
        p, late = _payload(rng.choice(cards), now, rng, late_ratio, with_device_id)
        n_late += late
        if rng.random() < invalid_ratio:
            _corrupt(p, rng)
            n_invalid += 1
        payloads.append(p)

    n_burst = 0
    if burst_card:
        card = next((c for c in cards if c["card_id"] == burst_card), None)
        if card is None:
            raise ValueError(f"burst_card inconnue : {burst_card} (attendu C00000 à C{n_cards - 1:05d})")
        burst = _burst(card, now, rng, burst_size, with_device_id)
        payloads += burst
        n_burst = len(burst)

    n_dup = int(len(payloads) * dup_ratio)
    payloads += rng.sample(payloads, n_dup)  # redelivery : même txn_id, nouvel offset
    rng.shuffle(payloads)

    summary = {"messages": len(payloads), "invalid": n_invalid, "duplicates": n_dup,
               "late": n_late, "burst": n_burst, "sink": sink}
    if sink == "kafka":
        _send_kafka(payloads, topic)
        summary["topic"] = topic
    else:
        summary["files"] = _write_files(payloads, files, landing, state_dir, now, with_headers)
    return summary


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--files", type=int, default=1)
    ap.add_argument("--landing", default=LANDING)
    ap.add_argument("--state-dir", default=STATE_DIR)
    ap.add_argument("--invalid-ratio", type=float, default=0.02)
    ap.add_argument("--dup-ratio", type=float, default=0.01)
    ap.add_argument("--late-ratio", type=float, default=0.03)
    ap.add_argument("--with-device-id", action="store_true")
    ap.add_argument("--with-headers", action="store_true")
    ap.add_argument("--burst-card", default=None)
    ap.add_argument("--burst-size", type=int, default=6)
    ap.add_argument("--sink", choices=["files", "kafka"], default="files")
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--seed", type=int, default=None)
    a, _ = ap.parse_known_args(argv)
    print(json.dumps(generate(
        n=a.n, files=a.files, landing=a.landing, state_dir=a.state_dir,
        invalid_ratio=a.invalid_ratio, dup_ratio=a.dup_ratio, late_ratio=a.late_ratio,
        with_device_id=a.with_device_id, with_headers=a.with_headers, burst_card=a.burst_card, burst_size=a.burst_size,
        sink=a.sink, topic=a.topic, seed=a.seed,
    ), indent=2))


if __name__ == "__main__":
    main()