"""Модель линий, поездов и весов станций."""

import csv
import math
from dataclasses import dataclass, field


@dataclass
class Station:
    station_id: int
    line_id: int
    name: str
    lat: float
    lon: float
    seq: int
    weight: float = 1.0


@dataclass
class LineModel:
    line_id: int
    stations: list
    model: str
    num_cars: int
    seated_per_car: int
    total_per_car: int
    headway_min: float
    ride_sec: float
    cycle_sec: float = field(default=0.0)
    train_ids: list = field(default_factory=list)

    def pos_offset(self, seq: int) -> float:
        """Время хода от конечной до станции seq (1-based), сек."""
        return (seq - 1) * self.ride_sec

    def arrival_at(self, seq: int, ts_epoch: float) -> tuple:
        """Следующий поезд на станции seq после ts_epoch -> (train_id, время прибытия).

        Парк равномерно распределён по циклу: прибытия следуют формуле
        base_k + pos(s) + m*cycle, где base_k = k*headway, cycle = T*headway.
        Поэтому следующий поезд = ceil(u/headway) % T, u = ts - pos(s).
        """
        headway = self.headway_min * 60.0
        pos = self.pos_offset(seq)
        m = math.ceil((ts_epoch - pos) / headway)
        idx = m % len(self.train_ids)
        return self.train_ids[idx], m * headway + pos


def load_stations(csv_path: str) -> list:
    with open(csv_path, "r", encoding="utf-8-sig") as fh:
        return [
            Station(
                station_id=int(r["station_id"]),
                line_id=int(r["line_id"]),
                name=r["name"],
                lat=float(r["lat"]),
                lon=float(r["lon"]),
                seq=int(r["seq"]),
            )
            for r in csv.DictReader(fh)
        ]


def apply_weights(stations: list, center_lat: float, center_lon: float):
    """Центральные станции оживлённее: вес ~ 1/(1+расстояние до центра, км)."""
    for s in stations:
        dlat = (s.lat - center_lat) * 111.0
        dlon = (s.lon - center_lon) * 111.0 * math.cos(math.radians(s.lat))
        dist_km = math.sqrt(dlat * dlat + dlon * dlon)
        s.weight = 1.0 / (1.0 + dist_km) + 0.05


def build_fleet(stations: list, cfg: dict) -> dict:
    """LineModel по каждой линии: парк, цикл движения, id поездов."""
    ride_sec = cfg["ride_step_min"] * 60.0
    turnback_sec = cfg["turnback_min"] * 60.0
    by_line = {}
    for s in stations:
        by_line.setdefault(s.line_id, []).append(s)

    lines = {}
    for line_id, sts in sorted(by_line.items()):
        sts.sort(key=lambda s: s.seq)
        p = cfg["lines"][line_id]
        n = len(sts)
        if line_id == 5:  # Кольцевая: составы идут по кругу без оборота
            cycle = ride_sec * (n + 1)
        else:
            cycle = 2 * ride_sec * (n - 1) + 2 * turnback_sec
        headway = p["headway_min"] * 60.0
        trains_count = max(2, round(cycle / headway))
        lines[line_id] = LineModel(
            line_id=line_id,
            stations=sts,
            model=p["model"],
            num_cars=p["num_cars"],
            seated_per_car=p["seated_per_car"],
            total_per_car=p["total_per_car"],
            headway_min=p["headway_min"],
            ride_sec=ride_sec,
            cycle_sec=cycle,
            train_ids=[line_id * 10000 + i for i in range(1, trains_count + 1)],
        )
    return lines


def fleet_rows(fleet: dict) -> list:
    """Строки для вставки в metro.trains."""
    rows = []
    for lm in fleet.values():
        for tid in lm.train_ids:
            rows.append((tid, lm.line_id, lm.num_cars, lm.seated_per_car, lm.total_per_car))
    return rows
