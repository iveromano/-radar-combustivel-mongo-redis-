"""
Redis index bootstrap
=====================

Creates the RediSearch index used by the dashboard and a couple of
TimeSeries metadata entries. Safe to re-run: existing indexes are
detected and skipped.

Run once after ``docker-compose up -d``:

    python init/redis_indexes.py
"""
from __future__ import annotations

import os
import sys

import redis

# Allow direct execution
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import (  # noqa: E402
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
    RedisKeys,
    configure_logging,
)


log = configure_logging("redis-indexes")


def _connect() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=REDIS_DB,
        decode_responses=True,
    )


def create_postos_index(r: redis.Redis) -> None:
    """
    RediSearch index over ``doc:posto:*`` hashes.

    Fields chosen to support the queries we need:
      * full-text search on bandeira / nome_fantasia / cidade / bairro
      * tag filtering by uf / bandeira (exact match, low cardinality)
      * geo filter by lat/lon
      * numeric filter by ativo (0/1)
    """
    try:
        r.execute_command("FT.INFO", RedisKeys.IDX_POSTOS)
        log.info("Indice %s ja existe -- mantendo.", RedisKeys.IDX_POSTOS)
        return
    except redis.ResponseError:
        pass

    args = [
        "FT.CREATE",
        RedisKeys.IDX_POSTOS,
        "ON", "HASH",
        "PREFIX", "1", RedisKeys.POSTO_DOC_PREFIX,
        "SCHEMA",
        "nome_fantasia", "TEXT", "WEIGHT", "2.0", "SORTABLE",
        "bandeira", "TAG", "SORTABLE",
        "cidade", "TEXT", "SORTABLE",
        "bairro", "TEXT", "SORTABLE",
        "estado", "TAG", "SORTABLE",
        "uf", "TAG", "SORTABLE",
        "ativo", "NUMERIC",
        "lat", "NUMERIC",
        "lon", "NUMERIC",
    ]
    r.execute_command(*args)
    log.info("Indice %s criado.", RedisKeys.IDX_POSTOS)


def warm_up_metrics(r: redis.Redis) -> None:
    """Seed the metrics hash so the dashboard does not show ``--``."""
    r.hsetnx(RedisKeys.METRICS_HASH, "processed_total", "0")
    r.hsetnx(RedisKeys.METRICS_HASH, "errors", "0")
    r.hsetnx(RedisKeys.METRICS_HASH, "last_event_collection", "")


def main() -> None:
    r = _connect()
    log.info("Conectando em redis://%s:%s/%s", REDIS_HOST, REDIS_PORT, REDIS_DB)
    r.ping()
    create_postos_index(r)
    warm_up_metrics(r)
    log.info("Pronto. Voce ja pode iniciar o consumer.")


if __name__ == "__main__":
    main()
