"""
Simulateur CDC au format Debezium  (bank-realtime-lakehouse).

Il écrit des événements JSON (une ligne par événement) dans :
    /Volumes/bank/ops/landing/cdc/customers/batch_NN.json
    /Volumes/bank/ops/landing/cdc/accounts/batch_NN.json

Format d'un événement (enveloppe Debezium simplifiée) :
    {"before": {...} | null, "after": {...} | null,
     "op": "r" | "c" | "u" | "d", "ts_ms": 1730448000000,
     "source": {"connector": "postgresql", "db": "corebank", "table": "customers", "lsn": ...}}

    r = lecture du snapshot initial, c = création, u = mise à jour, d = suppression.
    ts_ms = heure du commit dans la base source (c'est elle qui sert de séquence).

Les scénarios sont DÉTERMINISTES, pour que tu puisses vérifier les résultats attendus :
    batch 1 : snapshot initial (20 clients, 20 comptes, op = 'r')
    batch 2 : mises à jour, créations, suppressions
    batch 3 : événements EN RETARD (ts_ms plus ancien que des événements déjà reçus)

Usage dans un notebook du Git folder :
    import sys; sys.path.append("../generators")
    from cdc_simulator import write_batch, describe
    describe()          # affiche les scénarios
    write_batch(1)      # puis lance le pipeline
    write_batch(2)      # puis lance le pipeline et fais les contrôles
    write_batch(3)      # puis lance le pipeline et refais les contrôles

Un fichier déjà écrit n'est jamais réécrit : Auto Loader / read_files ne relit pas
un fichier dont le nom a déjà été ingéré, même s'il a été modifié.
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

BASE_PATH = "/Volumes/bank/ops/landing/cdc"
T0 = datetime(2026, 11, 1, 8, 0, tzinfo=timezone.utc)  # heure du snapshot initial

FIRST = ["Camille", "Lucas", "Emma", "Hugo", "Léa", "Nathan", "Chloé", "Louis",
         "Manon", "Jules", "Inès", "Adam", "Sarah", "Noah", "Jade", "Paul",
         "Zoé", "Arthur", "Lina", "Gabriel", "Alice", "Tom"]
LAST = ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit",
        "Durand", "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre", "Michel",
        "Garcia", "David", "Bertrand", "Roux", "Vincent", "Fournier", "Morel", "Girard"]
CITIES = [("Paris", "FR"), ("Berlin", "DE"), ("Madrid", "ES"), ("Milan", "IT"), ("Lille", "FR")]


def _at(day: float) -> datetime:
    return T0 + timedelta(days=day)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _customer(i: int) -> dict:
    city, country = CITIES[(i - 1) % len(CITIES)]
    return {
        "customer_id": f"C{i:03d}",
        "full_name": f"{FIRST[i - 1]} {LAST[i - 1]}",
        "email": f"client{i:03d}@example.com",
        "address": f"{i} rue de la Banque, {city}",
        "country": country,
    }


def _iban(i: int) -> str:
    # IBAN fictif (la clé de contrôle n'est pas valide)
    return f"FR76 3000 4000 {i:04d} 0000 {i:04d} {i:03d}"


def _account(i: int, rng: random.Random) -> dict:
    balance = 1000.00 if i == 1 else round(rng.uniform(50, 20000), 2)
    return {
        "account_id": f"A{i:03d}",
        "customer_id": f"C{i:03d}",
        "iban": _iban(i),
        "status": "ACTIVE",
        "balance": balance,
    }


# --------------------------------------------------------------------------
# Scénarios : (clé, jour du commit, op, changements, batch d'arrivée)
# Le "jour du commit" donne ts_ms ; le "batch d'arrivée" dit dans quel fichier
# l'événement est livré. Un événement du batch 3 avec un jour plus ancien
# qu'un événement du batch 2 est un événement EN RETARD.
# --------------------------------------------------------------------------
CUSTOMER_CHANGES = [
    ("C001", 1, "u", {"address": "8 cours Lafayette, Lyon"}, 2),
    ("C001", 3, "u", {"address": "3 quai du Port, Marseille"}, 2),
    ("C002", 1, "u", {"email": "c002.nouveau@example.com"}, 2),
    ("C003", 2, "d", None, 2),
    ("C004", 4, "u", {"address": "4 avenue Neuve (v3), Milan"}, 2),
    ("C004", 2, "u", {"address": "4 via Roma (v2), Milan"}, 3),        # EN RETARD
    ("C005", 2, "u", {"address": "5 Unter den Linden, Berlin", "country": "DE"}, 2),
    ("C021", 1, "c", _customer(21), 2),
    ("C022", 5, "c", _customer(22), 3),
]

ACCOUNT_CHANGES = [
    ("A001", 1, "u", {"balance": 750.00}, 2),
    ("A001", 3, "u", {"balance": 1200.00}, 2),
    ("A002", 2, "u", {"status": "BLOCKED"}, 2),
    ("A003", 3, "d", None, 2),
    ("A003", 2, "u", {"balance": 42.00}, 3),                            # EN RETARD, avant la suppression
    ("A004", 4, "u", {"balance": 500.00}, 2),
    ("A004", 2, "u", {"balance": 999.00}, 3),                           # EN RETARD
    ("A021", 5, "c", None, 3),                                          # rempli plus bas
]


def _event(table: str, op: str, before, after, dt: datetime) -> dict:
    return {
        "before": before,
        "after": after,
        "op": op,
        "ts_ms": _ms(dt),
        "source": {"connector": "postgresql", "db": "corebank", "schema": "public",
                   "table": table, "lsn": _ms(dt) // 1000},
    }


def _build(table: str, initial: dict, changes: list) -> dict:
    """Rejoue l'historique dans l'ordre des commits pour remplir before/after,
    puis range chaque événement dans son batch d'arrivée."""
    state = {k: dict(v) for k, v in initial.items()}
    batches = {1: [_event(table, "r", None, dict(v), T0) for v in initial.values()], 2: [], 3: []}

    for key, day, op, chg, batch in sorted(changes, key=lambda c: (c[0], c[1])):
        dt = _at(day)
        before = dict(state[key]) if key in state else None
        if op == "d":
            after = None
            state.pop(key, None)
        elif op == "c":
            after = dict(chg)
            state[key] = dict(after)
        else:  # "u"
            after = {**state[key], **chg}
            state[key] = dict(after)
        batches[batch].append(_event(table, op, before, after, dt))
    return batches


def _all_batches() -> dict:
    rng = random.Random(42)
    customers = {f"C{i:03d}": _customer(i) for i in range(1, 21)}
    accounts = {f"A{i:03d}": _account(i, rng) for i in range(1, 21)}
    acc_changes = [
        (k, d, op, (_account(21, rng) if k == "A021" else c), b)
        for (k, d, op, c, b) in ACCOUNT_CHANGES
    ]
    out = {"customers": _build("customers", customers, CUSTOMER_CHANGES),
           "accounts": _build("accounts", accounts, acc_changes)}
    # Dans un même fichier, les événements ne sont pas triés : on mélange.
    shuffler = random.Random(7)
    for table in out.values():
        for evts in table.values():
            shuffler.shuffle(evts)
    return out


def write_batch(batch: int, base_path: str = BASE_PATH) -> None:
    if batch not in (1, 2, 3):
        raise ValueError("batch doit valoir 1, 2 ou 3")
    data = _all_batches()
    for table in ("customers", "accounts"):
        folder = f"{base_path}/{table}"
        path = f"{folder}/batch_{batch:02d}.json"
        if os.path.exists(path):
            raise FileExistsError(
                f"{path} existe déjà. Ne réécris pas un batch : le pipeline ne le relirait pas.")
        os.makedirs(folder, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for e in data[table][batch]:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        ops = {}
        for e in data[table][batch]:
            ops[e["op"]] = ops.get(e["op"], 0) + 1
        print(f"{path} : {len(data[table][batch])} événements {ops}")


def describe() -> None:
    print("Clients (SCD 2 dans bank.silver.customers, SCD 1 dans bank.silver.customers_current)")
    for k, d, op, c, b in CUSTOMER_CHANGES:
        late = "  <-- EN RETARD" if b == 3 and op != "c" else ""
        print(f"  batch {b} | {k} | op={op} | commit {(_at(d)):%d/%m %H:%M} | {c if op != 'c' else 'création'}{late}")
    print("Comptes (SCD 1 dans bank.silver.accounts)")
    for k, d, op, c, b in ACCOUNT_CHANGES:
        late = "  <-- EN RETARD" if b == 3 and op != "c" else ""
        print(f"  batch {b} | {k} | op={op} | commit {(_at(d)):%d/%m %H:%M} | {c if op != 'c' else 'création'}{late}")


if __name__ == "__main__":
    describe()