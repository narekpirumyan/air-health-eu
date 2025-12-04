"""
Helpers to ingest and tidy the EDGAR NUTS2 emissions workbook.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

EDGAR_SHEETS: Dict[str, str] = {
    "Fossil CO2 AR5": "fossil_co2",
    "CH4_AR5": "ch4",
    "N2O_AR5": "n2o",
    "F-gas AR5": "f_gas",
}

DEFAULT_SECTOR_GROUPS: Dict[str, str] = {
    "Agriculture": "agriculture",
    "Buildings": "buildings",
    "Energy": "energy",
    "Industry": "industry",
    "Transport": "transport",
    "Dom_Avi": "transport",
    "Dom_Ship": "transport",
    "Waste": "waste",
}

# Path relative to project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # mvp/src/pipeline -> mvp/src -> mvp -> root
DEFAULT_OUTPUT = PROJECT_ROOT / "mvp" / "data" / "processed" / "emissions_nuts2.parquet"


def _read_sheet(
    workbook_path: Path,
    sheet_name: str,
    gas_label: str,
) -> pd.DataFrame:
    """
    Load a single sheet from the EDGAR workbook and return a tidy dataframe.
    """

    df = pd.read_excel(workbook_path, sheet_name=sheet_name, skiprows=5)
    df = df.dropna(subset=["NUTS 2"])

    id_cols = [
        "Substance",
        "ISO",
        "Country",
        "NUTS 2",
        "NUTS 2 desc",
        "Sector",
    ]
    value_cols = [col for col in df.columns if col.startswith("Y_")]

    tidy = df.melt(
        id_vars=id_cols,
        value_vars=value_cols,
        var_name="year",
        value_name="emissions_kt_co2e",
    )

    tidy["year"] = tidy["year"].str.replace("Y_", "", regex=False).astype(int)
    tidy["gas_sheet"] = gas_label
    tidy = tidy.rename(
        columns={
            "Substance": "gas",
            "ISO": "country_iso",
            "Country": "country_name",
            "NUTS 2": "nuts_id",
            "NUTS 2 desc": "nuts_label",
            "Sector": "sector",
        }
    )

    tidy["nuts_id"] = tidy["nuts_id"].str.strip().str.upper()
    tidy["country_iso"] = tidy["country_iso"].str.strip().str.upper()
    tidy["gas"] = tidy["gas"].fillna(tidy["gas_sheet"])
    tidy = tidy.drop(columns=["gas_sheet"])

    return tidy


def ingest_edgar_emissions(
    workbook_path: Path,
    output_path: Optional[Path] = None,
    sector_groups: Optional[Dict[str, str]] = None,
    sheets: Optional[Iterable[str]] = None,
) -> Path:
    """
    Convert the EDGAR workbook into a tidy parquet file.

    Parameters
    ----------
    workbook_path:
        Path to the EDGAR xlsx file.
    output_path:
        Where to write the tidy parquet (defaults to data/processed/...).
    sector_groups:
        Optional override mapping sector -> high-level bucket.
    sheets:
        Optional subset of sheet names to process (uses EDGAR_SHEETS keys).
    """

    workbook_path = Path(workbook_path)
    if output_path is None:
        output_path = DEFAULT_OUTPUT
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sheets_to_use = sheets or EDGAR_SHEETS.keys()
    frames: list[pd.DataFrame] = []

    for sheet in sheets_to_use:
        gas_label = EDGAR_SHEETS.get(sheet, sheet)
        frames.append(_read_sheet(workbook_path, sheet, gas_label))

    if not frames:
        raise ValueError("No EDGAR sheets were processed.")

    combined = pd.concat(frames, ignore_index=True)
    mapper = sector_groups or DEFAULT_SECTOR_GROUPS
    combined["sector_group"] = combined["sector"].map(mapper).fillna("other")

    combined = combined[
        [
            "nuts_id",
            "nuts_label",
            "country_iso",
            "country_name",
            "year",
            "gas",
            "sector",
            "sector_group",
            "emissions_kt_co2e",
        ]
    ]

    combined = combined.dropna(subset=["emissions_kt_co2e"])
    combined.to_parquet(output_path, index=False)

    return output_path


def _parse_args():
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest the EDGAR NUTS2 workbook into a tidy parquet dataset.",
    )
    parser.add_argument(
        "--workbook",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "emissions" / "EDGARv8.0_GHG_by substance_GWP100_AR5_NUTS2_1990_2022.xlsx",
        help="Path to the EDGAR workbook.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path where the parquet output should be written.",
    )
    return parser


def main() -> None:
    parser = _parse_args()
    args = parser.parse_args()
    ingest_edgar_emissions(args.workbook, args.output)


if __name__ == "__main__":
    main()

