"""Record linkage simples entre a base de busca e o CNEFE (camada silver).

Para cada endereço da busca:
1. Bloqueio: só compara com o CNEFE do mesmo município.
2. Logradouro: escolhe o(s) nome(s) mais parecido(s) (rapidfuzz, SCORER) acima de LIMIAR_LOGRADOURO.
   Se nenhum passar, tenta o bairro (ou o próprio nome) contra a localidade do CNEFE.
3. Setor: entre os endereços escolhidos, prefere mesmo tipo, mesmo bairro (localidade) e mesmo CEP
   (quando não é genérico), depois o número mais próximo; o setor mais frequente vence.
4. Status: ENCONTRADO se o setor vencedor tem pelo menos DOMINANCIA dos candidatos; senão AMBIGUO
   (o setor mais provável é preenchido mesmo assim); sem candidato, NAO_ENCONTRADO.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import functions as F
from rapidfuzz import fuzz, process

from eng_data_challenge.main import PROJECT_ROOT, create_spark
from eng_data_challenge.schemas import BUSCA_COLUMNS

LOGGER = logging.getLogger(__name__)

SCORER = fuzz.token_sort_ratio
LIMIAR_LOGRADOURO = 85
LIMIAR_LOCALIDADE = 90
DOMINANCIA = 0.8


def melhores(texto: object, opcoes: list[str], limiar: float) -> tuple[float, list[str]]:
    """Retorna o maior score e todas as opções empatadas nele (vazio se nada passar do limiar)."""
    if not isinstance(texto, str) or not opcoes:
        return 0.0, []
    resultados = process.extract(texto, opcoes, scorer=SCORER, limit=None, score_cutoff=limiar)
    if not resultados:
        return 0.0, []
    topo = resultados[0][1]
    return topo, [opcao for opcao, score, _ in resultados if score == topo]


def escolher_candidatos(busca: pd.DataFrame, nomes: pd.DataFrame) -> pd.DataFrame:
    """Para cada consulta, o logradouro (ou localidade) do CNEFE mais parecido no mesmo município."""
    por_municipio = {
        campo: grupo.groupby("cod_municipio_6")["valor"].apply(list).to_dict()
        for campo, grupo in nomes.groupby("campo")
    }
    escolhas = []
    for q in busca.itertuples():
        tentativas = [
            ("logradouro", "nome_chave", q.nome_chave, LIMIAR_LOGRADOURO),
            ("localidade", "localidade_norm", q.bairro_norm, LIMIAR_LOCALIDADE),
            ("localidade", "localidade_norm", q.nome_chave, LIMIAR_LOCALIDADE),
        ]
        for metodo, campo, texto, limiar in tentativas:
            opcoes = por_municipio[campo].get(q.cod_municipio_6, [])
            score, valores = melhores(texto, opcoes, limiar)
            if valores:
                escolhas += [(q.query_id, q.cod_municipio_6, metodo, campo, v, score) for v in valores]
                break
    return pd.DataFrame(
        escolhas, columns=["query_id", "cod_municipio_6", "metodo", "campo", "valor", "score"]
    )


def preferir(candidatos: pd.DataFrame, filtro: pd.Series) -> pd.DataFrame:
    """Aplica o filtro só se sobrar algum candidato."""
    return candidatos[filtro] if filtro.any() else candidatos


def resolver_setor(q: Any, candidatos: pd.DataFrame) -> tuple[str | None, str, int]:
    """Escolhe o setor entre os endereços candidatos de uma consulta."""
    if candidatos.empty:
        return None, "NAO_ENCONTRADO", 0
    candidatos = preferir(candidatos, candidatos["tipo_norm"] == q.tipo_norm)
    candidatos = preferir(candidatos, candidatos["localidade_norm"] == q.bairro_norm)
    if not q.cep_generico:
        candidatos = preferir(candidatos, candidatos["cep"] == q.cep)
    if q.metodo == "logradouro" and pd.notna(q.numero) and candidatos["numero"].notna().any():
        distancia = (candidatos["numero"] - q.numero).abs()
        candidatos = candidatos[distancia == distancia.min()]

    frequencia = candidatos["setor"].value_counts(normalize=True).sort_index()
    frequencia = frequencia.sort_values(ascending=False, kind="stable")
    status = "ENCONTRADO" if frequencia.iloc[0] >= DOMINANCIA else "AMBIGUO"
    return frequencia.index[0], status, len(frequencia)


def run(root: Path) -> pd.DataFrame:
    spark = create_spark("local[*]", 48)
    try:
        busca = spark.read.parquet(str(root / "data" / "silver" / "busca")).toPandas()
        municipios = busca["cod_municipio_6"].unique().tolist()
        cnefe = (
            spark.read.parquet(str(root / "data" / "silver" / "cnefe"))
            .where(F.col("cod_municipio_6").isin(municipios))
            .select("cod_municipio_6", "nome_chave", "localidade_norm", "tipo_norm", "cep",
                    "numero", "setor")
        )

        #  Nomes distintos por município e escolha por similaridade
        nomes = (
            cnefe.select("cod_municipio_6", F.lit("nome_chave").alias("campo"),
                         F.col("nome_chave").alias("valor"))
            .unionByName(cnefe.select("cod_municipio_6", F.lit("localidade_norm").alias("campo"),
                                      F.col("localidade_norm").alias("valor")))
            .where(F.col("valor").isNotNull()).distinct().toPandas()
        )
        escolhas = escolher_candidatos(busca, nomes)

        #  Busca no CNEFE,  endereços dos nomes escolhidos
        escolhas_spark = spark.createDataFrame(escolhas)
        enderecos = None
        for campo in ("nome_chave", "localidade_norm"):
            parte = (
                escolhas_spark.where(F.col("campo") == campo)
                .select("query_id", "cod_municipio_6", F.col("valor").alias(campo))
                .join(cnefe, ["cod_municipio_6", campo])
                .select("query_id", "tipo_norm", "localidade_norm", "cep", "numero", "setor")
            )
            enderecos = parte if enderecos is None else enderecos.unionByName(parte)
        enderecos = enderecos.toPandas()
    finally:
        spark.stop()

    # Setor e status por consulta.
    resumo_escolha = escolhas.groupby("query_id").agg(
        metodo=("metodo", "first"), score=("score", "first"),
        nome_casado=("valor", lambda v: " | ".join(sorted(v))),
    )
    busca = busca.merge(resumo_escolha, on="query_id", how="left")
    por_consulta = dict(tuple(enderecos.groupby("query_id")))
    vazio = enderecos.iloc[0:0]
    busca[["setor_censitario_encontrado", "status_vinculacao", "n_setores_candidatos"]] = [
        resolver_setor(q, por_consulta.get(q.query_id, vazio)) for q in busca.itertuples()
    ]

    resultado = busca.sort_values("source_row_number")[
        [*BUSCA_COLUMNS, "setor_censitario_encontrado", "status_vinculacao", "metodo", "score",
         "nome_casado", "n_setores_candidatos"]
    ]
    saida = root / "output"
    saida.mkdir(exist_ok=True)
    resultado.to_csv(saida / "resultado.csv", sep=";", index=False)
    resultado.to_parquet(saida / "resultado.parquet", index=False)

    LOGGER.info("Status:\n%s", resultado["status_vinculacao"].value_counts().to_string())
    LOGGER.info("Método:\n%s", resultado["metodo"].value_counts(dropna=False).to_string())
    LOGGER.info("Resultado gravado em %s", saida)
    return resultado


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    run(PROJECT_ROOT)


if __name__ == "__main__":
    main()
