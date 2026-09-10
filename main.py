"""Serverless Cloud Run HTTP service for automated sports betting odds ingestion.

Supports Flask in container deployments and Starlette/WSGI for local execution.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import UTC, datetime
from typing import Any

from app.config.config import gcp_service_accnt, my_api_key
from app.query_betting_api import DEFAULT_SPORTS, OddsQuery

logger = logging.getLogger("odds_collector")
logging.basicConfig(level=logging.INFO)


def _safe_error_message(error: Exception) -> str:
    """Remove query-string credentials before writing an exception to logs."""
    return re.sub(r"([?&]apiKey=)[^&\s]+", r"\1[REDACTED]", str(error))


def run_ingestion_pipeline(
    sports: list[str] | None = None,
    test_mode: bool = False,
) -> tuple[dict[str, Any], int]:
    """Execute odds collection and BigQuery loading."""
    if not my_api_key:
        return {
            "status": "error",
            "message": "THE_ODDS_API_KEY environment variable is not configured.",
        }, 500

    sports_list = sports or list(DEFAULT_SPORTS)

    try:
        query = OddsQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
        odds_df = query.get_new_odds(sports=sports_list, test_mode=test_mode)

        rows_count = len(odds_df)
        if rows_count > 0:
            query.write_to_bq(odds_df)

        return {
            "status": "success",
            "rows_ingested": rows_count,
            "sports": sports_list,
            "timestamp": datetime.now(UTC).isoformat(),
            "quota_info": query.last_quota_info,
        }, 200
    except Exception as exc:
        logger.error("Ingestion failed: %s", _safe_error_message(exc))
        return {
            "status": "error",
            "message": _safe_error_message(exc),
            "timestamp": datetime.now(UTC).isoformat(),
        }, 500


# Check available web frameworks
try:
    from flask import Flask, jsonify, request

    app = Flask(__name__)

    @app.route("/health", methods=["GET"])
    def health_check():
        return jsonify({"status": "healthy", "service": "sports-odds-collector"}), 200

    @app.route("/", methods=["GET", "POST"])
    def trigger_ingestion():
        req_sports = request.args.get("sports")
        sports_list = [s.strip() for s in req_sports.split(",") if s.strip()] if req_sports else None
        test_mode = request.args.get("test_mode", "").lower() in ("true", "1")
        result, code = run_ingestion_pipeline(sports=sports_list, test_mode=test_mode)
        return jsonify(result), code

    def run_server(port: int = 8080):
        app.run(host="0.0.0.0", port=port)

except ImportError:
    import uvicorn
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    async def health_check_route(req):
        return JSONResponse({"status": "healthy", "service": "sports-odds-collector"})

    async def trigger_ingestion_route(req):
        req_sports = req.query_params.get("sports")
        sports_list = [s.strip() for s in req_sports.split(",") if s.strip()] if req_sports else None
        test_mode = req.query_params.get("test_mode", "").lower() in ("true", "1")
        result, code = run_ingestion_pipeline(sports=sports_list, test_mode=test_mode)
        return JSONResponse(result, status_code=code)

    app = Starlette(
        routes=[
            Route("/health", health_check_route, methods=["GET"]),
            Route("/", trigger_ingestion_route, methods=["GET", "POST"]),
        ]
    )

    def run_server(port: int = 8080):
        uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    run_server(port)

