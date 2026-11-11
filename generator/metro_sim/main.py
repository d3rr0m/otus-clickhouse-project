"""Точка входа симулятора.

Режим работы:
  1. При старте — backfill: генерируется история за `backfill_hours` (по умолчанию
     72 ч = 3 суток) до текущего момента, одним burst-потоком.
  2. Затем — real-time: каждую реальную секунду генерируется и отправляется в Kafka
     порция событий, «случившихся» в эту секунду.

Модель: пассажиропоток — ПЕРЕЕЗДЫ (trip):
  'in'  — вход на станцию A, посадка на ближайший поезд линии по расписанию;
  'out' — выход на станции B после поездки тем же поездом (тот же train_id и card_hash).
Заполняемость поезда = running sum (+1/-1) по минутам.
"""

import heapq
import json
import logging
import math
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from .config import load_config
from .model import apply_weights, build_fleet, fleet_rows, load_stations

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("metro_sim")

_stop = False


def _sigterm(signum, frame):
    global _stop
    _stop = True


def fmt_ts(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{int(dt.microsecond / 1000):03d}"


class Emitter:
    def __init__(self, cfg, fleet, producer):
        self.cfg = cfg
        self.fleet = fleet
        self.producer = producer
        self.topic = cfg["kafka_topic"]
        self.profile = cfg["hourly_profile"]
        self.profile_sum = sum(self.profile)
        self.ride_sec = cfg["ride_step_min"] * 60.0
        self.stations = [s for lm in fleet.values() for s in lm.stations]
        self.weights = [s.weight for s in self.stations]
        self.pending = []  # heap: (ts высадки, счётчик, событие)
        self._ctr = 0
        self.sent = 0
        self.errors = 0

    def _err_cb(self, err, msg):
        if err is not None:
            self.errors += 1
            log.warning("kafka delivery error: %s", err)

    def _produce(self, rec):
        while True:
            try:
                self.producer.produce(
                    self.topic, value=json.dumps(rec).encode(), on_delivery=self._err_cb
                )
                self.sent += 1
                return
            except BufferError:
                self.producer.poll(0.1)

    def _rec(self, ts, station, direction, train_id, card):
        return {
            "event_time": fmt_ts(ts),
            "station_id": station.station_id,
            "turnstile_id": random.randint(1, 8),
            "direction": direction,
            "train_id": train_id,
            "card_hash": card,
        }

    def _pick_destination(self, station, line_model):
        """Станция высадки: дальность ~|gauss| станций, обе стороны, с зажимом границ."""
        n = len(line_model.stations)
        k = 1 + int(abs(random.gauss(0, 4.5)))
        direction = 1 if random.random() < 0.5 else -1
        target = min(max(station.seq + direction * k, 1), n)
        if target == station.seq:
            target = min(max(station.seq - direction * k, 1), n)
        return line_model.stations[target - 1]

    def _sample_count(self, sim_ts: float) -> int:
        """Сколько посадок в этой секунде: доля часа из профиля суток / 3600 секунд,
        дробная вероятность — целая часть + Бернулли от остатка."""
        hour = datetime.fromtimestamp(sim_ts, tz=timezone.utc).hour
        rate = (
            self.cfg["events_per_day"] * self.profile[hour] / self.profile_sum / 3600.0
        )
        rate *= random.uniform(0.8, 1.2)
        n = int(rate)
        if random.random() < rate - n:
            n += 1
        return n

    def emit_second(self, sim_ts: float):
        # 1) высадки, наступившие к этой секунде (поездки, начатые ранее)
        while self.pending and self.pending[0][0] <= sim_ts:
            _, _, rec = heapq.heappop(self.pending)
            self._produce(rec)

        # 2) новые посадки этой секунды
        for _ in range(self._sample_count(sim_ts)):
            st = random.choices(self.stations, weights=self.weights, k=1)[0]
            lm = self.fleet[st.line_id]
            request_ts = sim_ts + random.uniform(0, 0.999)
            train_id, in_ts = lm.arrival_at(st.seq, request_ts)
            dest = self._pick_destination(st, lm)
            out_ts = in_ts + abs(dest.seq - st.seq) * self.ride_sec + random.uniform(30, 120)
            card = f"{random.getrandbits(128):032x}"
            self._produce(self._rec(in_ts, st, "in", train_id, card))
            self._ctr += 1
            heapq.heappush(
                self.pending, (out_ts, self._ctr, self._rec(out_ts, dest, "out", train_id, card))
            )
        self.producer.poll(0)

    def emit_range(self, ts_from: float, ts_to: float):
        """Backfill: [ts_from, ts_to) помсекундно, без сна."""
        t = math.floor(ts_from)
        while t < ts_to and not _stop:
            self.emit_second(t)
            if t % 3600 == 0:
                log.info("backfill progress: sent=%d sim=%s", self.sent, fmt_ts(t))
            t += 1

    def flush(self):
        self.producer.flush(10)


def seed_trains(cfg, fleet):
    """Загрузка парка поездов в metro.trains (ReplacingMergeTree, идемпотентно)."""
    rows = fleet_rows(fleet)
    values = ",".join(f"({tid},{lid},{nc},{sp},{tp})" for tid, lid, nc, sp, tp in rows)
    sql = (
        "INSERT INTO metro.trains "
        "(train_id, line_id, num_cars, seated_per_car, total_per_car) VALUES " + values
    )
    deadline = time.time() + 120
    while time.time() < deadline:
        try:
            resp = requests.post(
                cfg["ch_url"],
                params={"database": "metro"},
                data=sql,
                auth=(cfg["ch_user"], cfg["ch_password"]),
                timeout=30,
            )
            if resp.ok:
                log.info("fleet seeded: %d trains", len(rows))
                return
            log.error("CH seed failed: %s %s", resp.status_code, resp.text[:200])
        except requests.RequestException as exc:
            log.warning("CH not ready: %s", exc)
        time.sleep(5)
    sys.exit("could not seed trains into ClickHouse")


def main():
    signal.signal(signal.SIGTERM, _sigterm)
    cfg = load_config(os.path.join(os.path.dirname(__file__), "..", "config.yaml"))
    stations = load_stations(cfg["stations_csv"])
    apply_weights(stations, cfg["center"]["lat"], cfg["center"]["lon"])
    fleet = build_fleet(stations, cfg)
    total_trains = sum(len(lm.train_ids) for lm in fleet.values())
    log.info(
        "model: %d stations, %d lines, %d trains; %d boardings/day, backfill %s h",
        len(stations), len(fleet), total_trains,
        cfg["events_per_day"], cfg["backfill_hours"],
    )

    seed_trains(cfg, fleet)

    producer = Producer({
        "bootstrap.servers": cfg["kafka_bootstrap"],
        "linger.ms": 10,
        "batch.num.messages": 5000,
        "acks": 1,
        "compression.type": "lz4",
    })
    emitter = Emitter(cfg, fleet, producer)
    end_ts = time.time()

    if cfg["backfill_hours"] > 0:
        t0 = time.time()
        emitter.emit_range(end_ts - cfg["backfill_hours"] * 3600, end_ts)
        log.info(
            "backfill done: %d events over %s h in %.0f s wall",
            emitter.sent, cfg["backfill_hours"], time.time() - t0,
        )

    log.info("entering realtime loop: one batch per second")
    next_sec = math.ceil(end_ts)
    last_stat = time.time()
    while not _stop:
        delay = next_sec - time.time()
        if delay > 0:
            time.sleep(delay)
        while next_sec <= time.time() and not _stop:
            emitter.emit_second(next_sec)
            next_sec += 1
        if time.time() - last_stat >= 15:
            log.info(
                "sent=%d pending=%d errors=%d sim=%s",
                emitter.sent, len(emitter.pending), emitter.errors, fmt_ts(next_sec),
            )
            last_stat = time.time()

    emitter.flush()
    log.info("stopped. total sent=%d errors=%d", emitter.sent, emitter.errors)


if __name__ == "__main__":
    main()
