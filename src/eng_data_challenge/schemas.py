from __future__ import annotations

from collections.abc import Sequence
from typing import Any

BUSCA_COLUMNS = (
    "consulta_logradouro",
    "consulta_numero",
    "consulta_bairro",
    "consulta_cep",
    "consulta_complemento",
    "consulta_municipio",
)

CNEFE_COLUMNS = (
    "COD_UNICO_ENDERECO",
    "COD_UF",
    "COD_MUNICIPIO",
    "COD_DISTRITO",
    "COD_SUBDISTRITO",
    "COD_SETOR",
    "NUM_QUADRA",
    "NUM_FACE",
    "CEP",
    "DSC_LOCALIDADE",
    "NOM_TIPO_SEGLOGR",
    "NOM_TITULO_SEGLOGR",
    "NOM_SEGLOGR",
    "NUM_ENDERECO",
    "DSC_MODIFICADOR",
    "NOM_COMP_ELEM1",
    "VAL_COMP_ELEM1",
    "NOM_COMP_ELEM2",
    "VAL_COMP_ELEM2",
    "NOM_COMP_ELEM3",
    "VAL_COMP_ELEM3",
    "NOM_COMP_ELEM4",
    "VAL_COMP_ELEM4",
    "NOM_COMP_ELEM5",
    "VAL_COMP_ELEM5",
    "LATITUDE",
    "LONGITUDE",
    "NV_GEO_COORD",
    "COD_ESPECIE",
    "DSC_ESTABELECIMENTO",
    "COD_INDICADOR_ESTAB_ENDERECO",
    "COD_INDICADOR_CONST_ENDERECO",
    "COD_INDICADOR_FINALIDADE_CONST",
    "COD_TIPO_ESPECI",
)


def string_schema(columns: Sequence[str]) -> Any:
    """Cria schema Spark"""

    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([StructField(column, StringType(), nullable=True) for column in columns])


def busca_schema() -> Any:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    fields = [
        StructField("query_id", StringType(), nullable=False),
        StructField("source_row_number", LongType(), nullable=False),
    ]
    fields.extend(StructField(column, StringType(), nullable=True) for column in BUSCA_COLUMNS)
    return StructType(fields)

