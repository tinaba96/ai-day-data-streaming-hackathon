"""Synthetic payment-event generator.

Produces a steady baseline stream of payment events, optionally injecting:
  - a volume spike (one merchant, many small txns, decline storm),
  - a value outlier (one user, very large txn),
  - a geo-velocity violation (one user, multiple cities in seconds).

Outputs either to stdout (JSON lines), to a file, or directly to Snowflake
via the Snowpipe Streaming Ingest SDK if --sink snowflake is chosen.

Usage:
    python payment_generator.py --mode normal --rate 50 --duration 60
    python payment_generator.py --mode spike  --rate 800 --duration 30
    python payment_generator.py --mode geo    --user user-1042
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator


CITIES: list[tuple[str, str, float, float]] = [
    ("US", "Seattle",       47.6062, -122.3321),
    ("US", "Austin",        30.2672,  -97.7431),
    ("US", "Chicago",       41.8781,  -87.6298),
    ("US", "Boston",        42.3601,  -71.0589),
    ("US", "New York",      40.7128,  -74.0060),
    ("FR", "Paris",         48.8566,    2.3522),
    ("JP", "Tokyo",         35.6762,  139.6503),
    ("DE", "Berlin",        52.5200,   13.4050),
    ("BR", "Sao Paulo",    -23.5505,  -46.6333),
    ("SG", "Singapore",      1.3521,  103.8198),
]

CATEGORIES: dict[str, tuple[float, float]] = {
    # category: (mean_amount, stddev)
    "grocery":      (45.0,   20.0),
    "ride_share":   (22.0,   10.0),
    "restaurant":   (60.0,   30.0),
    "streaming":    (11.0,    3.0),
    "luxury_goods": (1800.0, 600.0),
    "electronics":  (350.0, 150.0),
}

DECLINE_REASONS = [
    "insufficient_funds",
    "do_not_honor",
    "expired_card",
    "fraud_suspected",
]


@dataclass
class Event:
    def to_dict(self) -> dict:
        return self.__dict__


def make_event(
    *,
    user_id: str | None = None,
    merchant_id: str | None = None,
    category: str | None = None,
    city_choice: tuple[str, str, float, float] | None = None,
    force_decline: bool = False,
    amount_override: float | None = None,
) -> dict:
    category = category or random.choice(list(CATEGORIES))
    mean, std = CATEGORIES[category]
    amount = amount_override if amount_override is not None else max(0.5, random.gauss(mean, std))
    country, city, lat, lon = city_choice or random.choice(CITIES)
    declined = force_decline or random.random() < 0.04
    return {
        "event_id":          f"evt-{uuid.uuid4().hex[:12]}",
        "event_ts":          datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "user_id":           user_id or f"user-{random.randint(1000, 9999)}",
        "merchant_id":       merchant_id or f"M-{random.randint(1000, 9999)}",
        "merchant_category": category,
        "amount":            round(amount, 2),
        "currency":          "USD" if country == "US" else {"FR": "EUR", "JP": "JPY", "DE": "EUR", "BR": "BRL", "SG": "SGD"}[country],
        "country":           country,
        "city":              city,
        "lat":               lat,
        "lon":               lon,
        "payment_method":    random.choice(["card", "wallet", "bank_transfer"]),
        "status":            "DECLINED" if declined else "APPROVED",
        "decline_reason":    random.choice(DECLINE_REASONS) if declined else None,
    }


def normal_stream(rate_per_sec: int) -> Iterator[dict]:
    interval = 1.0 / max(rate_per_sec, 1)
    while True:
        yield make_event()
        time.sleep(interval)


def spike_stream(rate_per_sec: int, merchant: str = "M-7741") -> Iterator[dict]:
    """Volume + decline-rate spike on a single merchant."""
    interval = 1.0 / max(rate_per_sec, 1)
    seattle = ("US", "Seattle", 47.6062, -122.3321)
    while True:
        yield make_event(
            merchant_id=merchant,
            category="grocery",
            city_choice=seattle,
            force_decline=random.random() < 0.45,
            amount_override=round(random.uniform(8, 12), 2),
        )
        time.sleep(interval)


def geo_velocity_burst(user_id: str) -> Iterator[dict]:
    """Same user appearing on three continents within a minute."""
    burst_cities = [
        ("US", "Seattle", 47.6062, -122.3321),
        ("FR", "Paris",   48.8566,    2.3522),
        ("JP", "Tokyo",   35.6762,  139.6503),
    ]
    for c in burst_cities:
        yield make_event(user_id=user_id, category="luxury_goods", city_choice=c, amount_override=5000)
        time.sleep(15)


def emit(events: Iterator[dict], duration: float, sink) -> None:
    deadline = time.time() + duration if duration > 0 else float("inf")
    n = 0
    for ev in events:
        sink(ev)
        n += 1
        if time.time() > deadline:
            break
    print(f"[generator] emitted {n} events", file=sys.stderr)


def stdout_sink(ev: dict) -> None:
    print(json.dumps(ev))


def file_sink(path: str):
    fh = open(path, "a", encoding="utf-8")
    def write(ev: dict) -> None:
        fh.write(json.dumps(ev) + "\n")
        fh.flush()
    return write


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=["normal", "spike", "geo"], default="normal")
    p.add_argument("--rate", type=int, default=50, help="events per second")
    p.add_argument("--duration", type=float, default=60, help="seconds; 0 = forever")
    p.add_argument("--user", default="user-1042", help="user_id for --mode geo")
    p.add_argument("--merchant", default="M-7741", help="merchant_id for --mode spike")
    p.add_argument("--out", default="-", help="output file path; '-' for stdout")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    random.seed()
    sink = stdout_sink if args.out == "-" else file_sink(args.out)

    if args.mode == "normal":
        events = normal_stream(args.rate)
    elif args.mode == "spike":
        events = spike_stream(args.rate, merchant=args.merchant)
    else:
        events = geo_velocity_burst(args.user)

    emit(events, args.duration, sink)


if __name__ == "__main__":
    main()
