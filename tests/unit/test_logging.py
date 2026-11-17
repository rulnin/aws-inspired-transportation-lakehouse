import json
import logging

from lakehouse.common.logging import JsonFormatter


def test_json_formatter_includes_message_and_pipeline_context() -> None:
    record = logging.LogRecord(
        name="lakehouse.ingestion",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="source accepted",
        args=(),
        exc_info=None,
    )
    record.pipeline_run_id = "run-123"
    record.source_month = "2024-01"
    record.component = "ingestion"
    record.status = "success"

    payload = json.loads(JsonFormatter().format(record))

    assert payload["message"] == "source accepted"
    assert payload["pipeline_run_id"] == "run-123"
    assert payload["source_month"] == "2024-01"
    assert payload["component"] == "ingestion"
    assert payload["status"] == "success"
