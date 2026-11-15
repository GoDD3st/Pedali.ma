#!/usr/bin/env python3
"""Generateur de trajets Pedali.ma (remplacement du producer.py officiel).
Usage:
  python producer.py --mode file  --n 50000          -> trips_history.jsonl
  python producer.py --mode kafka --rate 20          -> topic Kafka 'trips'
"""
import argparse, json, os, random, time
from datetime import datetime, timedelta, timezone

FMT = "%Y-%m-%dT%H:%M:%SZ"
ZONES = ["gare", "universite", "plage", "centre_affaires", "residentiel", "tourisme"]
ZONE_W = [1, 2, 1, 3, 6, 2]
CITIES = {  # nom: (code, lat, lon, nb_stations, quartiers)
    "Casablanca": ("CAS", 33.5731, -7.5898, 160, ["Maarif", "Anfa", "Ain Sebaa", "Hay Hassani", "Sidi Maarouf", "Habous"]),
    "Rabat":      ("RAB", 34.0209, -6.8416, 90,  ["Agdal", "Hassan", "Souissi", "Yacoub El Mansour", "Ocean"]),
    "Marrakech":  ("MAR", 31.6295, -7.9811, 90,  ["Gueliz", "Medina", "Hivernage", "Palmeraie", "Massira"]),
    "Tanger":     ("TNG", 35.7595, -5.8340, 60,  ["Malabata", "Centre", "Marshan", "Boukhalef"]),
}
FAULTY_VEHICLE = "V-SC-0666"
DRAIN_STATION = "S-CAS-007"
PREFIX = {"BIKE": "BK", "EBIKE": "EB", "SCOOTER": "SC"}
BASE = {"BIKE": 3.0, "EBIKE": 5.0, "SCOOTER": 5.0}
PER_MIN = {"BIKE": 0.4, "EBIKE": 0.7, "SCOOTER": 0.8}


def make_stations(path):
    rng = random.Random(42)
    rows = []
    for city, (code, lat, lon, nb, districts) in CITIES.items():
        for i in range(1, nb + 1):
            zone = "gare" if i == 1 else rng.choices(ZONES, ZONE_W)[0]
            d = rng.choice(districts)
            rows.append({"station_id": f"S-{code}-{i:03d}", "name": f"Station {d} {i}",
                         "city": city, "district": d, "zone_type": zone,
                         "capacity": rng.choice([10, 15, 20, 25, 30]),
                         "latitude": round(lat + rng.uniform(-0.04, 0.04), 6),
                         "longitude": round(lon + rng.uniform(-0.04, 0.04), 6)})
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"{len(rows)} stations ecrites dans {path}")


def load_stations(path):
    if not os.path.exists(path):
        make_stations(path)
    with open(path, encoding="utf-8") as f:
        st = [json.loads(l) for l in f if l.strip()]
    by_city = {}
    for s in st:
        by_city.setdefault(s["city"], []).append(s)
    return by_city


def new_trip(ts, by_city):
    city = random.choices(list(by_city), [len(v) for v in by_city.values()])[0]
    sts = by_city[city]
    h = ts.hour
    zone = lambda z: [s for s in sts if s["zone_type"] == z] or sts
    start = random.choice(sts)
    if 6 <= h < 10 and random.random() < 0.6:
        start = random.choice(zone("residentiel"))
    elif 16 <= h < 20 and random.random() < 0.6:
        start = random.choice(zone("centre_affaires"))
    if city == "Casablanca" and random.random() < 0.04:       # station qui se vide
        start = next(s for s in sts if s["station_id"] == DRAIN_STATION)
    if 6 <= h < 10 and start["zone_type"] == "residentiel" and random.random() < 0.7:
        end = random.choice(zone("centre_affaires"))
    elif 16 <= h < 20 and start["zone_type"] == "centre_affaires" and random.random() < 0.7:
        end = random.choice(zone("residentiel"))
    else:
        end = random.choice(sts)
    while end["station_id"] in (start["station_id"], DRAIN_STATION):
        end = random.choice(sts)
    if city == "Marrakech" and random.random() < 0.03:         # boucle touristique A->B->C->A
        loop = (zone("tourisme") + sts)[:3]
        i = random.randrange(3)
        start, end = loop[i], loop[(i + 1) % 3]
    vt = random.choices(["BIKE", "EBIKE", "SCOOTER"], [4, 4, 2])[0]
    vid = f"V-{PREFIX[vt]}-{random.randint(1, 999):04d}"
    if random.random() < 0.015:                                # vehicule en panne
        vt, vid = "SCOOTER", FAULTY_VEHICLE
    dur = int(min(max(random.gauss(1200, 500), 120), 4500))
    return {"trip_id": f"T-{random.randint(0, 99999999):08d}", "user_id": f"U-{random.randint(10000, 29999)}",
            "vehicle_id": vid, "vehicle_type": vt, "start": start["station_id"], "end": end["station_id"],
            "city": city, "dur": dur, "dist": round(dur / 60 * 0.25 * random.uniform(0.8, 1.2), 1),
            "bat": None if vt == "BIKE" else random.randint(15, 100)}


def event(t, status, ts):
    ended = status == "ENDED"
    return {"trip_id": t["trip_id"], "user_id": t["user_id"], "vehicle_id": t["vehicle_id"],
            "vehicle_type": t["vehicle_type"], "start_station": t["start"],
            "end_station": t["end"] if ended else None, "city": t["city"], "status": status,
            "duration_s": t["dur"] if ended else None, "distance_km": t["dist"] if ended else None,
            "battery_pct": t["bat"],
            "price_mad": round(BASE[t["vehicle_type"]] + t["dur"] / 60 * PER_MIN[t["vehicle_type"]], 2) if ended else None,
            "event_time": ts.strftime(FMT)}


def gen_file(n, out, by_city):
    now = datetime.now(timezone.utc)
    events = []  # (heure_d_arrivee, evenement)
    while len(events) < n:
        ts = now - timedelta(seconds=random.uniform(0, 14 * 86400))
        t = new_trip(ts, by_city)
        seq = [("RESERVED", ts)]
        unl = ts + timedelta(seconds=random.randint(30, 300))
        if random.random() < 0.08:
            seq.append(("CANCELLED", ts + timedelta(seconds=random.randint(30, 200))))
        else:
            seq.append(("UNLOCKED", unl))
            if t["vehicle_id"] == FAULTY_VEHICLE or random.random() < 0.03:
                seq.append(("FAULT_REPORTED", unl + timedelta(seconds=random.randint(60, 600))))
            else:
                seq.append(("ENDED", unl + timedelta(seconds=t["dur"])))
        for status, et in seq:
            late = timedelta(seconds=random.uniform(60, 900)) if random.random() < 0.05 else timedelta(0)
            e = event(t, status, et)
            events.append((et + late, e))
            if random.random() < 0.02:
                events.append((et + late, e))
    events.sort(key=lambda x: x[0])
    with open(out, "w", encoding="utf-8") as f:
        for _, e in events[:n]:
            f.write(json.dumps(e) + "\n")
    print(f"{n} evenements ecrits dans {out}")


def run_kafka(rate, bootstrap, topic, by_city):
    from kafka import KafkaProducer
    p = KafkaProducer(bootstrap_servers=bootstrap,
                      value_serializer=lambda v: json.dumps(v).encode(),
                      key_serializer=lambda k: k.encode())
    print(f"Envoi de {rate} evt/s vers {topic} (Ctrl+C pour arreter)")
    while True:
        now = datetime.now(timezone.utc)
        t = new_trip(now, by_city)
        status = random.choices(["ENDED", "RESERVED", "UNLOCKED", "CANCELLED", "FAULT_REPORTED"], [60, 15, 10, 8, 7])[0]
        if t["vehicle_id"] == FAULTY_VEHICLE:
            status = "FAULT_REPORTED"
        et = now - timedelta(seconds=random.uniform(60, 900)) if random.random() < 0.05 else now
        e = event(t, status, et)
        p.send(topic, key=e["trip_id"], value=e)
        if random.random() < 0.02:
            p.send(topic, key=e["trip_id"], value=e)
        time.sleep(1 / rate)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["file", "kafka"], required=True)
    ap.add_argument("--n", type=int, default=50000)
    ap.add_argument("--rate", type=float, default=20)
    ap.add_argument("--out", default="trips_history.jsonl")
    ap.add_argument("--stations", default="stations.json")
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="trips")
    a = ap.parse_args()
    by_city = load_stations(a.stations)
    if a.mode == "file":
        gen_file(a.n, a.out, by_city)
    else:
        run_kafka(a.rate, a.bootstrap, a.topic, by_city)