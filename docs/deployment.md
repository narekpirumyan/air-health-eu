# Deployment & Operational Notes

## Streamlit app
1. Ensure the curated dataset is up to date:
   `ash
   python -m src.pipeline.ingest_emissions
   python -m src.pipeline.ingest_health
   python -m src.pipeline.harmonize
   `
2. Install dependencies (pip install -r requirements.txt).
3. Run locally with streamlit run app/main.py.
4. For Streamlit Community Cloud / Render / Azure App Service:
   - Point the service to pp/main.py.
   - Set the working directory to the repository root.
   - Persist data/curated/eu_climate_health.parquet as part of the repo or rebuild via the pipeline on startup.

## Data refresh automation
- Schedule a weekly GitHub Action (cron) that installs dependencies, re-runs the ingestion + harmonization modules, uploads refreshed parquet files as artifacts, and optionally commits them.
- Add secret EUROSTAT_EMAIL only if Eurostat quotas apply (not needed for current anonymous downloads).

## Testing & quality gates
- Run pytest before deployment to ensure key integrity checks (region-year uniqueness, non-negative populations, emissions totals).
- Consider adding 
uff/lack for linting in CI once the codebase grows.

## Caching & performance
- Streamlit caches heavy assets (GeoJSON + curated parquet) via st.cache_* to keep latency low.
- For multi-user deployments, mount a read-only volume for data/curated to avoid race conditions.

## Future hardening ideas
- Add automated data validation (e.g., great_expectations) before writing curated files.
- Parameterize the dashboard with URL query params for sharable views.
- Implement a feature flag to switch between total/per-capita metrics without recomputing data on the fly.
