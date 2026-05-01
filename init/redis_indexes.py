import os
from typing import Dict

from dotenv import load_dotenv
from pymongo import MongoClient
from redis import Redis
from redis.commands.search.field import (
    GeoField,
    NumericField,
    TagField,
    TextField,
)
from redis.commands.search.index_definition import (
    IndexDefinition,
    IndexType,
)

# =========================================================
# ENVIRONMENT
# =========================================================

# Load local env first
load_dotenv(".env.local")
load_dotenv()

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/?directConnection=true",
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_NAME = "radar_combustivel"

# Mongo collections
COL_POSTOS = "postos"
COL_LOCALIZACOES = "localizacoes_postos"
COL_EVENTOS_PRECO = "eventos_preco"
COL_AVALIACOES = "avaliacoes_interacoes"


# =========================================================
# SNAPSHOT: POSTOS + LOCALIZACAO + PRECO MAIS RECENTE
# =========================================================

def load_postos_snapshot() -> Dict[str, dict]:

    mongo = MongoClient(MONGO_URI)

    postos_col = mongo[DB_NAME][COL_POSTOS]
    local_col = mongo[DB_NAME][COL_LOCALIZACOES]
    preco_col = mongo[DB_NAME][COL_EVENTOS_PRECO]
    aval_col = mongo[DB_NAME][COL_AVALIACOES]

    postos = {}

    # -----------------------------------------------------
    # POSTOS BASE
    # -----------------------------------------------------

    for posto in postos_col.find():

        posto_id = str(posto["_id"])

        postos[posto_id] = {
            "posto_id": posto_id,
            "nome_fantasia": posto.get("nome_fantasia", ""),
            "bandeira": posto.get("bandeira", ""),
            "cnpj": posto.get("cnpj", ""),
            "cidade": posto.get("endereco", {}).get("cidade", ""),
            "estado": posto.get("endereco", {}).get("estado", ""),
            "bairro": posto.get("endereco", {}).get("bairro", ""),
            "logradouro": posto.get("endereco", {}).get("logradouro", ""),
            "ativo": int(bool(posto.get("ativo", True))),
            "telefone": posto.get("telefone", ""),
            "location": posto.get("location", {}),
            "precos": {},
            "media_avaliacao": 0.0,
            "total_avaliacoes": 0,
            "shares": 0,
        }

    # -----------------------------------------------------
    # LOCALIZACAO MAIS RECENTE
    # -----------------------------------------------------

    pipeline_local = [
        {"$sort": {"atualizado_em": -1}},
        {
            "$group": {
                "_id": "$posto_id",
                "geo": {"$first": "$geo"},
                "municipio": {"$first": "$municipio"},
                "bairro": {"$first": "$bairro"},
                "uf": {"$first": "$uf"},
            }
        },
    ]

    for row in local_col.aggregate(pipeline_local):

        posto_id = str(row["_id"])

        if posto_id not in postos:
            continue

        postos[posto_id]["geo"] = row.get("geo", {})
        postos[posto_id]["municipio"] = row.get("municipio", "")
        postos[posto_id]["bairro_geo"] = row.get("bairro", "")
        postos[posto_id]["uf"] = row.get("uf", "")

    # -----------------------------------------------------
    # PRECO MAIS RECENTE POR COMBUSTIVEL
    # -----------------------------------------------------

    pipeline_precos = [
        {"$sort": {"ocorrido_em": -1}},
        {
            "$group": {
                "_id": {
                    "posto_id": "$posto_id",
                    "combustivel": "$combustivel",
                },
                "preco_novo": {"$first": "$preco_novo"},
                "variacao_pct": {"$first": "$variacao_pct"},
                "fonte": {"$first": "$fonte"},
            }
        },
    ]

    for row in preco_col.aggregate(pipeline_precos):

        posto_id = str(row["_id"]["posto_id"])
        combustivel = row["_id"]["combustivel"]

        if posto_id not in postos:
            continue

        postos[posto_id]["precos"][combustivel] = {
            "preco": float(row.get("preco_novo", 0)),
            "variacao_pct": float(row.get("variacao_pct", 0)),
            "fonte": row.get("fonte", ""),
        }

    # -----------------------------------------------------
    # AVALIACOES
    # -----------------------------------------------------

    pipeline_avaliacoes = [
        {
            "$match": {
                "nota": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": "$posto_id",
                "media_avaliacao": {"$avg": "$nota"},
                "total_avaliacoes": {"$sum": 1},
            }
        },
    ]

    for row in aval_col.aggregate(pipeline_avaliacoes):

        posto_id = str(row["_id"])

        if posto_id not in postos:
            continue

        postos[posto_id]["media_avaliacao"] = round(
            float(row.get("media_avaliacao", 0)),
            2,
        )

        postos[posto_id]["total_avaliacoes"] = int(
            row.get("total_avaliacoes", 0)
        )

    # -----------------------------------------------------
    # SHARES / INTERACOES
    # -----------------------------------------------------

    pipeline_shares = [
        {
            "$match": {
                "tipo": "compartilhamento"
            }
        },
        {
            "$group": {
                "_id": "$posto_id",
                "shares": {"$sum": 1},
            }
        },
    ]

    for row in aval_col.aggregate(pipeline_shares):

        posto_id = str(row["_id"])

        if posto_id not in postos:
            continue

        postos[posto_id]["shares"] = int(row.get("shares", 0))

    return postos


# =========================================================
# MAIN
# =========================================================

def main() -> None:

    redis = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    snapshot = load_postos_snapshot()

    # =====================================================
    # HASHES
    # posto:{id}
    # =====================================================

    for posto_id, item in snapshot.items():

        coords = (
            item.get("location", {})
            .get("coordinates", [0, 0])
        )

        lon = coords[0]
        lat = coords[1]

        precos = item.get("precos", {})

        redis.hset(
            f"posto:{posto_id}",
            mapping={

                # IDs
                "posto_id": posto_id,

                # Dados básicos
                "nome_fantasia": item.get("nome_fantasia", ""),
                "bandeira": item.get("bandeira", ""),
                "cnpj": item.get("cnpj", ""),

                # Endereço
                "cidade": item.get("cidade", ""),
                "estado": item.get("estado", ""),
                "bairro": item.get("bairro", ""),
                "logradouro": item.get("logradouro", ""),

                # Status
                "ativo": item.get("ativo", 1),

                # Avaliações
                "media_avaliacao": item.get(
                    "media_avaliacao",
                    0.0,
                ),

                "total_avaliacoes": item.get(
                    "total_avaliacoes",
                    0,
                ),

                "shares": item.get("shares", 0),

                # Geo
                "location": f"{lon},{lat}",

                # Combustíveis
                "gasolina_comum": precos.get(
                    "GASOLINA_COMUM",
                    {},
                ).get("preco", 0),

                "gasolina_aditivada": precos.get(
                    "GASOLINA_ADITIVADA",
                    {},
                ).get("preco", 0),

                "etanol": precos.get(
                    "ETANOL",
                    {},
                ).get("preco", 0),

                "diesel_s10": precos.get(
                    "DIESEL_S10",
                    {},
                ).get("preco", 0),
            },
        )

        # =================================================
        # REDISTIMESERIES
        # =================================================

        metrics = [
            "views",
            "searches",
            "price_updates",
            "shares",
        ]

        for metric in metrics:

            ts_key = f"ts:posto:{posto_id}:{metric}"

            try:

                redis.execute_command(
                    "TS.CREATE",
                    ts_key,
                    "RETENTION",
                    604800000,
                    "LABELS",
                    "posto_id",
                    posto_id,
                    "metric",
                    metric,
                )

            except Exception:
                # already exists
                pass

    # =====================================================
    # REDISEARCH
    # =====================================================

    try:
        redis.execute_command(
            "FT.DROPINDEX",
            "idx:postos",
            "DD",
        )
    except Exception:
        pass

    redis.ft("idx:postos").create_index(
        fields=[

            TextField(
                "nome_fantasia",
                weight=2.0,
            ),

            TagField("bandeira"),

            TagField("cidade"),

            TagField("estado"),

            TagField("bairro"),

            NumericField(
                "media_avaliacao",
                sortable=True,
            ),

            NumericField(
                "total_avaliacoes",
                sortable=True,
            ),

            NumericField(
                "gasolina_comum",
                sortable=True,
            ),

            NumericField(
                "etanol",
                sortable=True,
            ),

            NumericField(
                "diesel_s10",
                sortable=True,
            ),

            GeoField("location"),
        ],
        definition=IndexDefinition(
            prefix=["posto:"],
            index_type=IndexType.HASH,
        ),
    )

    print(
        f"[REDIS] idx:postos criado com "
        f"{len(snapshot)} documentos."
    )


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    main()