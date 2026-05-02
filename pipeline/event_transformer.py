# event_transformer.py

from typing import Any, Dict


# =========================================================
# NORMALIZE
# =========================================================

def normalize_event(
    raw: Dict[str, Any],
    source: str,
) -> Dict[str, Any]:

    # =====================================================
    # PRICE EVENTS
    # =====================================================

    if source == "eventos_preco":

        ts = int(
            raw["ocorrido_em"]
            .timestamp() * 1000
        )

        return {

            "type": "price_update",

            "posto_id": str(
                raw["posto_id"]
            ),

            "combustivel": str(
                raw.get(
                    "combustivel",
                    "",
                )
            ).lower(),

            "preco_anterior": float(
                raw.get(
                    "preco_anterior",
                    0,
                )
            ),

            "preco_novo": float(
                raw.get(
                    "preco_novo",
                    0,
                )
            ),

            "variacao_pct": float(
                raw.get(
                    "variacao_pct",
                    0,
                )
            ),

            "fonte": raw.get(
                "fonte",
                "",
            ),

            "ts": ts,
        }

    # =====================================================
    # RATINGS / INTERACTIONS
    # =====================================================

    if source == "avaliacoes_interacoes":

        ts = int(
            raw["created_at"]
            .timestamp() * 1000
        )

        return {

            "type": str(
                raw.get(
                    "tipo",
                    "",
                )
            ).lower(),

            "posto_id": str(
                raw["posto_id"]
            ),

            "usuario_id": str(
                raw.get(
                    "usuario_id",
                    "",
                )
            ),

            "nota": raw.get(
                "nota"
            ),

            "comentario": raw.get(
                "comentario",
                "",
            ),

            "util_count": int(
                raw.get(
                    "util_count",
                    0,
                )
            ),

            "ts": ts,
        }

    # =====================================================
    # SEARCH EVENTS
    # =====================================================

    if source == "buscas_usuarios":

        ts = int(
            raw["consultado_em"]
            .timestamp() * 1000
        )

        return {

            "type": "search",

            "usuario_id": str(
                raw.get(
                    "usuario_id",
                    "",
                )
            ),

            "session_id": str(
                raw.get(
                    "session_id",
                    "",
                )
            ),

            "bairro": raw.get(
                "bairro",
                "desconhecido",
            ),

            "cidade": raw.get(
                "cidade",
                "",
            ),

            "estado": raw.get(
                "estado",
                "",
            ),

            "combustivel": str(
                raw.get(
                    "tipo_combustivel",
                    "",
                )
            ).lower(),

            "raio_km": int(
                raw.get(
                    "raio_km",
                    0,
                )
            ),

            "resultado_count": int(
                raw.get(
                    "resultado_count",
                    0,
                )
            ),

            "latencia_ms": int(
                raw.get(
                    "latencia_ms",
                    0,
                )
            ),

            "ts": ts,
        }

    raise ValueError(
        f"Fonte inválida: {source}"
    )


# =========================================================
# HASH KEY
# =========================================================

def hash_key(
    posto_id: str,
) -> str:

    return f"posto:{posto_id}"


# =========================================================
# TIMESERIES KEY
# =========================================================

def ts_key(
    posto_id: str,
    combustivel: str,
) -> str:

    return (
        f"ts:posto:{posto_id}:"
        f"{combustivel}"
    )


# =========================================================
# RANKING KEY
# =========================================================

def ranking_preco_key(
    combustivel: str,
    bairro: str,
) -> str:

    return (
        f"ranking:preco:"
        f"{combustivel}:"
        f"{bairro}"
    )


def ranking_variacao_key(
    combustivel: str,
) -> str:

    return (
        f"ranking:variacao:"
        f"{combustivel}"
    )
