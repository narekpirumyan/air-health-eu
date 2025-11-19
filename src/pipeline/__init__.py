"""
Data pipeline utilities (ingestion, cleaning) for the EU Climate-Health dashboard.
"""

from .ingest_emissions import ingest_edgar_emissions  # noqa: F401
from .ingest_health import (
    ingest_causes_of_death,
    ingest_hospital_discharges,
)  # noqa: F401

