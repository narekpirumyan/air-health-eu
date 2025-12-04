"""
Streamlit app for the EU Air & Health dashboard MVP.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # mvp/app -> mvp -> root
CURATED_PATH = PROJECT_ROOT / "mvp" / "data" / "curated" / "eu_climate_health.parquet"
GEOJSON_PATH = PROJECT_ROOT / "data" / "raw" / "geo" / "NUTS_RG_20M_2021_4326.geojson"

MAP_METRICS: Dict[str, str] = {
    "Total emissions (kt CO₂-eq)": "total_emissions_kt",
    "Per-capita emissions (tonnes)": "emissions_per_capita_tonnes",
    "COPD mortality rate (per 100k)": "cod_copd_rate",
    "Asthma mortality rate (per 100k)": "cod_asthma_rate",
    "Respiratory discharges (per 100k)": "discharge_all_resp_per_100k",
}

HEALTH_METRICS: Dict[str, str] = {
    "All respiratory mortality (per 100k)": "cod_all_resp_rate",
    "COPD mortality (per 100k)": "cod_copd_rate",
    "Asthma mortality (per 100k)": "cod_asthma_rate",
    "Respiratory discharges (per 100k)": "discharge_all_resp_per_100k",
    "COPD discharges (per 100k)": "discharge_copd_per_100k",
}


@st.cache_data
def load_curated() -> pd.DataFrame:
    df = pd.read_parquet(CURATED_PATH)
    df["region_label"] = df["nuts_label"].fillna(df["nuts_id"])
    df["region_display"] = df["nuts_id"] + " · " + df["region_label"]
    return df


@st.cache_resource
def load_geojson():
    with open(GEOJSON_PATH, "r", encoding="utf-8") as fh:
        geojson = json.load(fh)
    nuts2_features = [
        feat for feat in geojson["features"] if feat["properties"].get("LEVL_CODE") == 2
    ]
    return {"type": "FeatureCollection", "features": nuts2_features}


def build_map(df: pd.DataFrame, metric_col: str, metric_label: str):
    geojson = load_geojson()
    fig = px.choropleth(
        df,
        geojson=geojson,
        locations="nuts_id",
        featureidkey="properties.NUTS_ID",
        color=metric_col,
        hover_name="region_label",
        hover_data={
            "country_iso": True,
            metric_col: ":.2f",
        },
        color_continuous_scale="Tealrose",
        labels={metric_col: metric_label},
        title=f"{metric_label} ({int(df['year'].iloc[0])})",
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin=dict(r=0, l=0, t=40, b=0))
    return fig


def prepare_sector_long(df: pd.DataFrame) -> pd.DataFrame:
    sector_cols = [
        c for c in df.columns if c.startswith("emissions_") and c.endswith("_kt")
    ]
    long_df = df[
        ["region_label", "nuts_id", "country_iso", "total_emissions_kt"] + sector_cols
    ].melt(
        id_vars=["region_label", "nuts_id", "country_iso", "total_emissions_kt"],
        value_vars=sector_cols,
        var_name="sector_group",
        value_name="emissions_kt",
    )
    long_df["sector_group"] = (
        long_df["sector_group"]
        .str.replace("emissions_", "", regex=False)
        .str.replace("_kt", "", regex=False)
        .str.replace("_", " ")
        .str.title()
    )
    return long_df


def build_health_bar(df: pd.DataFrame, metric_col: str, metric_label: str):
    top = df.nlargest(8, metric_col)
    bottom = df.nsmallest(8, metric_col)
    stacked = pd.concat([top, bottom])
    fig = px.bar(
        stacked,
        x="region_label",
        y=metric_col,
        color="country_iso",
        title=f"{metric_label}: highest vs lowest regions",
        labels={metric_col: metric_label, "region_label": "Region"},
    )
    fig.update_layout(xaxis_tickangle=45, height=400)
    return fig


def main():
    st.set_page_config(page_title="EU Air & Health", layout="wide")
    st.title("EU Air & Health Dashboard")
    st.caption(
        "Explore how greenhouse gas emissions relate to respiratory health outcomes across EU NUTS2 regions."
    )

    data = load_curated()
    min_year, max_year = int(data["year"].min()), int(data["year"].max())

    with st.sidebar:
        st.header("Filters")
        selected_year = st.slider("Year", min_year, max_year, max_year)
        countries = sorted(data["country_iso"].unique())
        selected_countries = st.multiselect(
            "Countries",
            options=countries,
            default=countries,
        )
        region_options = dict(
            sorted(
                {row.region_display: row.nuts_id for row in data.itertuples()}.items()
            )
        )
        selected_region_labels = st.multiselect(
            "Focus regions (optional)",
            options=list(region_options.keys()),
        )
        selected_metric_label = st.selectbox("Map metric", list(MAP_METRICS.keys()))
        selected_health_label = st.selectbox(
            "Health metric", list(HEALTH_METRICS.keys())
        )

    filtered = data[data["year"] == selected_year].copy()
    if selected_countries:
        filtered = filtered[filtered["country_iso"].isin(selected_countries)]
    if selected_region_labels:
        selected_ids = [region_options[label] for label in selected_region_labels]
        filtered = filtered[filtered["nuts_id"].isin(selected_ids)]

    if filtered.empty:
        st.warning("No data for the selected filters.")
        return

    map_metric_col = MAP_METRICS[selected_metric_label]
    health_metric_col = HEALTH_METRICS[selected_health_label]

    col_map, col_health = st.columns((2, 1))
    with col_map:
        map_fig = build_map(filtered, map_metric_col, selected_metric_label)
        st.plotly_chart(map_fig, width="stretch")

    with col_health:
        health_fig = build_health_bar(filtered, health_metric_col, selected_health_label)
        st.plotly_chart(health_fig, width="stretch")
        key_stats = (
            filtered[[map_metric_col, health_metric_col]]
            .describe()
            .loc[["mean", "min", "max"]]
            .rename(index={"mean": "Mean", "min": "Min", "max": "Max"})
            .round(2)
        )
        st.dataframe(key_stats, height=250, width="stretch")

    st.subheader("Region comparison")
    comparison_cols = [
        "region_label",
        "country_iso",
        "total_emissions_kt",
        "emissions_per_capita_tonnes",
        map_metric_col,
        health_metric_col,
    ]
    unique_cols = list(dict.fromkeys(comparison_cols))
    comparison_df = (
        filtered[unique_cols]
        .sort_values(map_metric_col, ascending=False)
        .reset_index(drop=True)
    )
    st.dataframe(comparison_df, width="stretch")

    st.subheader("Sector breakdown")
    sector_long = prepare_sector_long(filtered)
    sector_fig = px.bar(
        sector_long,
        x="region_label",
        y="emissions_kt",
        color="sector_group",
        hover_name="country_iso",
        title="Emissions by sector group",
        labels={"emissions_kt": "Emissions (kt CO₂-eq)", "region_label": "Region"},
    )
    sector_fig.update_layout(xaxis_tickangle=45)
    st.plotly_chart(sector_fig, width="stretch")


if __name__ == "__main__":
    main()

