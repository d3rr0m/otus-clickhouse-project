import os
import yaml


def load_config(path=None):
    path = path or os.environ.get("SIM_CONFIG", "config.yaml")
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    cfg["kafka_bootstrap"] = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    cfg["kafka_topic"] = os.environ.get("KAFKA_TOPIC", "metro.turnstile.events")
    cfg["ch_url"] = os.environ.get("CH_URL", "http://ch-shard1-r1:8123/")
    cfg["ch_user"] = os.environ.get("CH_USER", "admin")
    cfg["ch_password"] = os.environ.get("CH_PASSWORD", "")

    for key, env in (
        ("events_per_day", "SIM_EVENTS_PER_DAY"),
        ("backfill_hours", "SIM_BACKFILL_HOURS"),
    ):
        if os.environ.get(env):
            cfg[key] = int(os.environ[env])
    return cfg
