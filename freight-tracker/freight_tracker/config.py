from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class KafkaTopic:
    name: str  # "movements" or "vstp"
    bootstrap: str
    username: str
    password: str
    group: str
    topic: str


@dataclass
class Config:
    base_dir: Path
    watch: list[str]
    cif_path: Path
    cif_s3_uri: str = ""
    cif_check_minutes: int = 30
    state_file: Path = Path("state.json")
    output_dir: Optional[Path] = None
    write_seconds: int = 60
    operators: dict = field(default_factory=dict)
    horizon_hours: float = 24
    max_rows: int = 9
    passed_linger_min: int = 5
    not_activated_grace_min: int = 30
    passed_after_estimate_min: int = 10
    http_host: str = "0.0.0.0"
    http_port: int = 8090
    max_rewind_minutes: int = 240
    topics: list[KafkaTopic] = field(default_factory=list)


def read_env(path: Path) -> dict[str, str]:
    env = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def _topic(env: dict[str, str], prefix: str, name: str) -> Optional[KafkaTopic]:
    keys = ["BOOTSTRAP", "USERNAME", "PASSWORD", "GROUP", "TOPIC"]
    if not all(env.get(f"{prefix}_{k}") for k in keys):
        return None
    return KafkaTopic(name, *(env[f"{prefix}_{k}"] for k in keys))


def load_config(path: Path) -> Config:
    path = Path(path).resolve()
    base = path.parent
    raw = tomllib.loads(path.read_text())
    t, h, k = raw.get("tracker", {}), raw.get("http", {}), raw.get("kafka", {})
    rel = lambda p: (base / p) if p else None  # noqa: E731

    cfg = Config(
        base_dir=base,
        watch=[w.upper() for w in t["watch"]],
        cif_path=rel(t.get("cif_path", "cif")),
        cif_s3_uri=t.get("cif_s3_uri", ""),
        cif_check_minutes=t.get("cif_check_minutes", 30),
        state_file=rel(t.get("state_file", "state.json")),
        output_dir=rel(t.get("output_dir", "")),
        write_seconds=t.get("write_seconds", 60),
        horizon_hours=t.get("horizon_hours", 24),
        max_rows=t.get("max_rows", 9),
        passed_linger_min=t.get("passed_linger_minutes", 5),
        not_activated_grace_min=t.get("not_activated_grace_minutes", 30),
        passed_after_estimate_min=t.get("passed_after_estimate_minutes", 10),
        http_host=h.get("host", "0.0.0.0"),
        http_port=h.get("port", 8090),
        max_rewind_minutes=k.get("max_rewind_minutes", 240),
    )
    ops_file = rel(t.get("operators_file", "operators.json"))
    if ops_file and ops_file.exists():
        cfg.operators = {k: v for k, v in json.loads(ops_file.read_text()).items() if not k.startswith("_")}
    secrets = rel(k.get("secrets_file", "secrets.env"))
    if secrets and secrets.exists():
        env = read_env(secrets)
        cfg.topics = [x for x in (_topic(env, "MVT", "movements"), _topic(env, "VSTP", "vstp")) if x]
    return cfg
