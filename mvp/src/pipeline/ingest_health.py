"""
Ingestion helpers for Eurostat respiratory health datasets.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

# Paths relative to project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # mvp/src/pipeline -> mvp/src -> mvp -> root
DEFAULT_CAUSES_INPUT = PROJECT_ROOT / "data" / "raw" / "health" / "hlth_cd_asdr2.tsv"
DEFAULT_DISCHARGES_INPUT = PROJECT_ROOT / "data" / "raw" / "health" / "hlth_co_disch1t.tsv"

DEFAULT_CAUSES_OUTPUT = PROJECT_ROOT / "mvp" / "data" / "processed" / "health_causes_of_death.parquet"
DEFAULT_DISCHARGES_OUTPUT = PROJECT_ROOT / "mvp" / "data" / "processed" / "health_hospital_discharges.parquet"


def _tidy_eurostat_tsv(path: Path, value_name: str) -> pd.DataFrame:
    """
    Convert Eurostat multi-dimension TSV into a tidy dataframe.
    """

    df = pd.read_csv(path, sep="\t")
    dimension_col = df.columns[0]
    dimension_names = dimension_col.split(",")
    dims = df[dimension_col].str.split(",", expand=True)
    dims.columns = dimension_names

    wide_values = df.drop(columns=[dimension_col])
    tidy = pd.concat([dims, wide_values], axis=1)
    year_columns = [c for c in wide_values.columns if c.strip().isdigit()]

    tidy = tidy.melt(
        id_vars=dimension_names,
        value_vars=year_columns,
        var_name="year",
        value_name=value_name,
    )

    tidy["year"] = tidy["year"].astype(str).str.strip().astype(int)
    tidy[value_name] = (
        tidy[value_name]
        .astype(str)
        .str.strip()
        .replace(":", pd.NA)
    )
    tidy[value_name] = pd.to_numeric(tidy[value_name], errors="coerce")

    if "geo\\TIME_PERIOD" in tidy.columns:
        tidy = tidy.rename(columns={"geo\\TIME_PERIOD": "geo"})

    tidy["geo"] = tidy["geo"].str.strip().str.upper()
    return tidy


def ingest_causes_of_death(
    tsv_path: Path = DEFAULT_CAUSES_INPUT,
    output_path: Optional[Path] = None,
) -> Path:
    """
    Tidy the age-standardised causes-of-death dataset.
    """

    output_path = Path(output_path or DEFAULT_CAUSES_OUTPUT)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tidy = _tidy_eurostat_tsv(tsv_path, "age_standardised_rate_per_100k")
    tidy = tidy.rename(
        columns={
            "freq": "frequency",
            "unit": "unit_code",
            "sex": "sex",
            "age": "age_group",
            "icd10": "icd10_group",
        }
    )

    tidy = tidy[
        [
            "geo",
            "year",
            "frequency",
            "unit_code",
            "sex",
            "age_group",
            "icd10_group",
            "age_standardised_rate_per_100k",
        ]
    ]

    tidy.dropna(subset=["age_standardised_rate_per_100k"], inplace=True)
    tidy.to_parquet(output_path, index=False)
    return output_path


def ingest_hospital_discharges(
    tsv_path: Path = DEFAULT_DISCHARGES_INPUT,
    output_path: Optional[Path] = None,
) -> Path:
    """
    Tidy the hospital discharge dataset for respiratory ICD10 codes.
    """

    output_path = Path(output_path or DEFAULT_DISCHARGES_OUTPUT)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tidy = _tidy_eurostat_tsv(tsv_path, "discharges")
    tidy = tidy.rename(
        columns={
            "freq": "frequency",
            "age": "age_group",
            "indic_he": "indicator",
            "unit": "unit_code",
            "sex": "sex",
            "icd10": "icd10_group",
        }
    )

    tidy = tidy[
        [
            "geo",
            "year",
            "frequency",
            "indicator",
            "unit_code",
            "sex",
            "age_group",
            "icd10_group",
            "discharges",
        ]
    ]

    tidy.dropna(subset=["discharges"], inplace=True)
    tidy.to_parquet(output_path, index=False)
    return output_path


def _parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest Eurostat respiratory health datasets into tidy parquet files.",
    )
    parser.add_argument(
        "--causes-tsv",
        type=Path,
        default=DEFAULT_CAUSES_INPUT,
        help="Path to hlth_cd_asdr2.tsv",
    )
    parser.add_argument(
        "--discharges-tsv",
        type=Path,
        default=DEFAULT_DISCHARGES_INPUT,
        help="Path to hlth_co_disch1t.tsv",
    )
    parser.add_argument(
        "--causes-output",
        type=Path,
        default=DEFAULT_CAUSES_OUTPUT,
        help="Output parquet path for causes of death dataset.",
    )
    parser.add_argument(
        "--discharges-output",
        type=Path,
        default=DEFAULT_DISCHARGES_OUTPUT,
        help="Output parquet path for hospital discharges dataset.",
    )
    return parser


def main() -> None:
    parser = _parse_args()
    args = parser.parse_args()
    ingest_causes_of_death(args.causes_tsv, args.causes_output)
    ingest_hospital_discharges(args.discharges_tsv, args.discharges_output)


if __name__ == "__main__":
    main()

