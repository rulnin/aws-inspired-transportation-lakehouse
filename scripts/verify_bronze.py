"""Check each selected Bronze object against its manifest and Parquet footer."""

from __future__ import annotations

import logging
import os

from lakehouse.common.logging import configure_logging
from lakehouse.ingestion.verification import verify_months

LOGGER = logging.getLogger(__name__)


def main() -> None:
    configure_logging(os.environ.get("LAKEHOUSE_LOG_LEVEL", "INFO").upper())
    verified = verify_months()
    print(f"Bronze verified for months: {verified}")


if __name__ == "__main__":
    main()
