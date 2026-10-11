"""Funções simples para ler os arquivos de entrada."""

import csv
import shutil
from pathlib import Path
from zipfile import ZipFile

from eng_data_challenge.schemas import BUSCA_COLUMNS


def find_file(directory, pattern):
    files = sorted(directory.glob(pattern))
    if len(files) != 1:
        raise ValueError(f"Esperado um arquivo {pattern} em {directory}; encontrados: {files}")
    return files[0]


def read_search_rows(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, delimiter=";")
        if tuple(next(reader)) != BUSCA_COLUMNS:
            raise ValueError("Cabeçalho da base de busca diferente do esperado.")
        rows = []
        for line_number, values in enumerate(reader, start=2):
            if len(values) != len(BUSCA_COLUMNS):
                raise ValueError(f"Quantidade de colunas inválida na linha {line_number}.")
            rows.append((f"Q{line_number - 1:04d}", line_number, *values))
    return rows


def extract_csv(zip_path, directory):
    with ZipFile(zip_path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError("O ZIP do CNEFE deve conter um CSV.")
        destination = directory / Path(members[0]).name
        with archive.open(members[0]) as source, destination.open("wb") as output:
            shutil.copyfileobj(source, output)
    return destination


def is_complete_parquet(path):
    return (path / "_SUCCESS").is_file() and any(path.rglob("*.parquet"))
