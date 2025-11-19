from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
CURATED = BASE / "data" / "curated" / "eu_climate_health.parquet"
EMISSIONS = BASE / "data" / "processed" / "emissions_nuts2.parquet"


def test_curated_columns_present():
    df = pd.read_parquet(CURATED)
    expected = {
        "nuts_id",
        "year",
        "total_emissions_kt",
        "emissions_per_capita_tonnes",
        "cod_all_resp_rate",
        "cod_copd_rate",
        "cod_asthma_rate",
        "discharge_all_resp_per_100k",
        "population",
    }
    missing = expected - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


def test_no_duplicate_region_year():
    df = pd.read_parquet(CURATED)
    duplicates = df.duplicated(subset=["nuts_id", "year"]).sum()
    assert duplicates == 0, f"Found {duplicates} duplicate region-year rows"


def test_population_positive():
    df = pd.read_parquet(CURATED)
    assert (df["population"] > 0).all()


def test_emissions_sector_totals_match():
    df = pd.read_parquet(EMISSIONS)
    totals = (
        df.groupby(["nuts_id", "year"])["emissions_kt_co2e"].sum().reset_index()
    )
    curated = pd.read_parquet(CURATED)[["nuts_id", "year", "total_emissions_kt"]]
    merged = totals.merge(curated, on=["nuts_id", "year"], how="inner")
    assert (
        (merged["emissions_kt_co2e"] - merged["total_emissions_kt"]).abs() < 1e-6
    ).all()

