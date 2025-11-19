"""
Create a curated dataset that combines emissions, health, and population metrics.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
from eurostat import get_data_df

PROCESSED_DIR = Path("data/processed")
CURATED_PATH = Path("data/curated/eu_climate_health.parquet")
POPULATION_PATH = PROCESSED_DIR / "population_nuts2.parquet"

EMISSIONS_PATH = PROCESSED_DIR / "emissions_nuts2.parquet"
CAUSES_PATH = PROCESSED_DIR / "health_causes_of_death.parquet"
DISCHARGES_PATH = PROCESSED_DIR / "health_hospital_discharges.parquet"

RESP_RATE_CODES: Dict[str, str] = {
    "J": "cod_all_resp_rate",
    "J09-J11": "cod_influenza_rate",
    "J12-J18": "cod_pneumonia_rate",
    "J40-J44_J47": "cod_copd_rate",
    "J45_J46": "cod_asthma_rate",
}

RESP_DISCHARGE_CODES: Dict[str, str] = {
    "J": "discharge_all_resp",
    "J00-J11": "discharge_upper_resp",
    "J12-J18": "discharge_pneumonia",
    "J20-J22": "discharge_bronchitis",
    "J40-J44_J47": "discharge_copd",
    "J45_J46": "discharge_asthma",
    "J60-J99": "discharge_other_resp",
}


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _download_population() -> pd.DataFrame:
    df = get_data_df("demo_r_pjangrp3", flags=True)
    value_cols = [c for c in df.columns if c.endswith("_value")]
    tidy = df.melt(
        id_vars=["freq", "sex", "unit", "age", "geo\\TIME_PERIOD"],
        value_vars=value_cols,
        var_name="year_raw",
        value_name="population",
    )
    tidy["year"] = tidy["year_raw"].str.replace("_value", "", regex=False).astype(int)
    tidy = tidy.rename(columns={"geo\\TIME_PERIOD": "geo"})
    tidy = tidy[
        (tidy["freq"] == "A")
        & (tidy["sex"] == "T")
        & (tidy["unit"] == "NR")
        & (tidy["age"] == "TOTAL")
    ]
    tidy = tidy[["geo", "year", "population"]]
    tidy["geo"] = tidy["geo"].str.strip().str.upper()
    tidy = tidy.dropna(subset=["population"])
    tidy["population"] = tidy["population"].astype(float)
    return tidy


def load_population(force_refresh: bool = False) -> pd.DataFrame:
    if POPULATION_PATH.exists() and not force_refresh:
        return pd.read_parquet(POPULATION_PATH)

    tidy = _download_population()
    _ensure_parent(POPULATION_PATH)
    tidy.to_parquet(POPULATION_PATH, index=False)
    return tidy


def _pivot_health(
    df: pd.DataFrame,
    mapping: Dict[str, str],
    value_col: str,
) -> pd.DataFrame:
    filtered = df[
        (df["frequency"] == "A")
        & (df["sex"] == "T")
        & (df["age_group"] == "TOTAL")
        & (df["icd10_group"].isin(mapping.keys()))
    ].copy()
    filtered["metric"] = filtered["icd10_group"].map(mapping)
    wide = (
        filtered.pivot_table(
            index=["geo", "year"],
            columns="metric",
            values=value_col,
            aggfunc="mean",
        )
        .reset_index()
        .rename(columns={"geo": "nuts_id"})
    )
    wide.columns.name = None
    return wide


def _prepare_emissions(emissions: pd.DataFrame) -> pd.DataFrame:
    meta = (
        emissions.groupby(["nuts_id", "year"])
        .agg(
            nuts_label=("nuts_label", "first"),
            country_iso=("country_iso", "first"),
            country_name=("country_name", "first"),
        )
        .reset_index()
    )

    sector_pivot = (
        emissions.pivot_table(
            index=["nuts_id", "year"],
            columns="sector_group",
            values="emissions_kt_co2e",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
    )
    sector_pivot.columns.name = None
    sector_cols = [c for c in sector_pivot.columns if c not in {"nuts_id", "year"}]
    sector_pivot = sector_pivot.rename(
        columns={col: f"emissions_{col}_kt" for col in sector_cols},
    )

    totals = (
        emissions.groupby(["nuts_id", "year"])
        .agg(total_emissions_kt=("emissions_kt_co2e", "sum"))
        .reset_index()
    )

    merged = meta.merge(totals, on=["nuts_id", "year"])
    merged = merged.merge(sector_pivot, on=["nuts_id", "year"], how="left")
    return merged


def build_curated_dataset(
    emissions_path: Path = EMISSIONS_PATH,
    cod_path: Path = CAUSES_PATH,
    discharges_path: Path = DISCHARGES_PATH,
    population_path: Optional[Path] = None,
    output_path: Path = CURATED_PATH,
) -> Path:
    emissions = pd.read_parquet(emissions_path)
    causes = pd.read_parquet(cod_path)
    discharges = pd.read_parquet(discharges_path)

    population = (
        pd.read_parquet(population_path)
        if population_path and Path(population_path).exists()
        else load_population()
    )
    population = population.rename(columns={"geo": "nuts_id"})

    emissions_agg = _prepare_emissions(emissions)
    causes_wide = _pivot_health(causes, RESP_RATE_CODES, "age_standardised_rate_per_100k")
    discharges_wide = _pivot_health(discharges, RESP_DISCHARGE_CODES, "discharges")

    combined = (
        emissions_agg.merge(population, on=["nuts_id", "year"], how="inner")
        .merge(causes_wide, on=["nuts_id", "year"], how="left")
        .merge(discharges_wide, on=["nuts_id", "year"], how="left")
    )

    combined["population"] = combined["population"].astype(float)
    combined["emissions_per_capita_tonnes"] = (
        combined["total_emissions_kt"] * 1000.0 / combined["population"]
    )

    discharge_cols = [col for col in combined.columns if col.startswith("discharge_")]
    for col in discharge_cols:
        combined[f"{col}_per_100k"] = (
            combined[col] / combined["population"] * 100000.0
        )

    _ensure_parent(output_path)
    combined.to_parquet(output_path, index=False)
    return output_path


def _parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        description="Build the curated EU climate-health dataset.",
    )
    parser.add_argument(
        "--emissions",
        type=Path,
        default=EMISSIONS_PATH,
        help="Path to processed emissions parquet.",
    )
    parser.add_argument(
        "--cod",
        type=Path,
        default=CAUSES_PATH,
        help="Path to causes of death parquet.",
    )
    parser.add_argument(
        "--discharges",
        type=Path,
        default=DISCHARGES_PATH,
        help="Path to hospital discharges parquet.",
    )
    parser.add_argument(
        "--population",
        type=Path,
        default=POPULATION_PATH,
        help="Optional pre-downloaded population parquet.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=CURATED_PATH,
        help="Path for the curated dataset parquet.",
    )
    parser.add_argument(
        "--refresh-population",
        action="store_true",
        help="Force refresh of the Eurostat population lookup.",
    )
    return parser


def main() -> None:
    parser = _parse_args()
    args = parser.parse_args()
    if args.refresh_population:
        load_population(force_refresh=True)
    build_curated_dataset(
        emissions_path=args.emissions,
        cod_path=args.cod,
        discharges_path=args.discharges,
        population_path=args.population,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()

