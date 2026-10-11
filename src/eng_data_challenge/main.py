"""Fase 0: leitura dos arquivos em data/ e conversão para data/parquet/."""

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from eng_data_challenge.io_utils import (
    extract_csv,
    find_file,
    is_complete_parquet,
    read_search_rows,
)
from eng_data_challenge.schemas import CNEFE_COLUMNS, busca_schema, string_schema

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_spark(master="local[*]", shuffle_partitions=48):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("eng-data-challenge")
        .master(master)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def convert_search(spark, source, output, mode):
    rows = read_search_rows(source)
    dataframe = spark.createDataFrame(rows, schema=busca_schema())
    dataframe.coalesce(1).write.mode(mode).parquet(str(output))


def convert_cnefe(spark, source, output, mode, temporary_directory, partitions):
    from pyspark.sql import functions as F

    # O Spark lê o CSV extraído; a pasta temporária é removida ao final.
    with tempfile.TemporaryDirectory(dir=temporary_directory) as temporary:
        csv_path = extract_csv(source, Path(temporary))
        dataframe = (
            spark.read.option("header", True)
            .option("sep", ";")
            .option("encoding", "UTF-8")
            .option("mode", "FAILFAST")
            .option("enforceSchema", False)
            .schema(string_schema(CNEFE_COLUMNS))
            .csv(str(csv_path))
            .withColumn("cod_municipio_6", F.substring("COD_MUNICIPIO", 1, 6))
        )
        (
            dataframe.repartition(partitions, "cod_municipio_6")
            .write.mode(mode)
            .partitionBy("cod_municipio_6")
            .parquet(str(output))
        )


def run(args):
    root = args.project_root.resolve()
    data = root / "data"
    parquet = data / "parquet"
    manifests = data / "manifests"
    temporary = data / ".tmp"
    for directory in (parquet, manifests, temporary):
        directory.mkdir(parents=True, exist_ok=True)

    sources = {}
    if args.only in ("all", "busca"):
        sources["busca"] = args.busca_csv or find_file(data, "*BASE_BUSCA.csv")
    if args.only in ("all", "cnefe"):
        sources["cnefe"] = args.cnefe_zip or find_file(data, "*.zip")
    outputs = {"busca": parquet / "base_busca", "cnefe": parquet / "cnefe_bahia"}
    mode = "overwrite" if args.overwrite else "errorifexists"

    manifest_path = manifests / "ingestion_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    manifest.setdefault("inputs", {})
    manifest.setdefault("outputs", {})

    spark = create_spark(args.master, args.shuffle_partitions)
    try:
        for name, source in sources.items():
            output = outputs[name]
            if is_complete_parquet(output) and not args.overwrite:
                print(f"Parquet já existe; pulando: {output}")
                continue

            print(f"Convertendo {source} para {output}")
            if name == "busca":
                convert_search(spark, source, output, mode)
            else:
                convert_cnefe(spark, source, output, mode, temporary, args.shuffle_partitions)

            count = spark.read.parquet(str(output)).count()
            manifest["inputs"][name] = {
                "path": str(source),
                "size_bytes": source.stat().st_size,
            }
            manifest["outputs"][name] = {"path": str(output), "row_count": count}
            manifest["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            print(f"Concluído: {count:,} registros.")
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Converte a base de busca e o CNEFE para Parquet.")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--busca-csv", type=Path)
    parser.add_argument("--cnefe-zip", type=Path)
    parser.add_argument("--only", choices=("all", "busca", "cnefe"), default="all")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--shuffle-partitions", type=int, default=48)
    parser.add_argument("--overwrite", action="store_true")
    run(parser.parse_args())


if __name__ == "__main__":
    main()
