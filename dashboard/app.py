"""LakePulse - a single web UI over the Kafka -> Flink -> Iceberg -> Trino pipeline.

Serves the dashboard page and a small JSON API that reads the Iceberg sink
through Trino and probes each pipeline component for the status strip.
"""

import os
import socket
import urllib.error
import urllib.request

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from trino.dbapi import connect

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "dashboard")
FLINK_URL = os.environ.get("FLINK_URL", "http://jobmanager:18081")
MINIO_URL = os.environ.get("MINIO_URL", "http://minio:9000")
ICEBERG_REST_URL = os.environ.get("ICEBERG_REST_URL", "http://iceberg-rest:8181")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "broker:29092")

TABLE = "iceberg.db.clickstream_sink"

app = FastAPI(title="LakePulse")


def query(sql):
    """Run one Trino query and return a list of row tuples."""
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg", schema="db")
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def http_ok(url, timeout=3):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return 200 <= r.status < 400
    except (urllib.error.URLError, urllib.error.HTTPError, OSError):
        return False


def tcp_ok(hostport, timeout=3):
    host, _, port = hostport.partition(":")
    try:
        with socket.create_connection((host, int(port or 0)), timeout=timeout):
            return True
    except OSError:
        return False


def flink_job():
    """Return (state, name) of the newest Flink job, or (None, None)."""
    try:
        with urllib.request.urlopen(f"{FLINK_URL}/jobs/overview", timeout=3) as r:
            import json

            jobs = json.loads(r.read()).get("jobs", [])
        if not jobs:
            return None, None
        job = max(jobs, key=lambda j: j.get("start-time", 0))
        return job.get("state"), job.get("name")
    except Exception:
        return None, None


@app.get("/api/health")
def health():
    """Status of each pipeline hop, for the status strip."""
    state, job_name = flink_job()

    trino_up = False
    try:
        query("SELECT 1")
        trino_up = True
    except Exception:
        pass

    def chip(label, ok, detail, pending=False):
        status = "good" if ok else ("warning" if pending else "critical")
        return {"label": label, "status": status, "detail": detail}

    flink_ok = state == "RUNNING"
    flink_pending = state in ("INITIALIZING", "CREATED", "RESTARTING")

    return {
        "components": [
            chip("Kafka broker", tcp_ok(KAFKA_BROKER), "reachable" if tcp_ok(KAFKA_BROKER) else "unreachable"),
            chip(
                "Flink job",
                flink_ok,
                (state or "no job submitted").title() if state else "no job submitted",
                pending=flink_pending,
            ),
            chip("Iceberg catalog", http_ok(f"{ICEBERG_REST_URL}/v1/config"), "REST catalog online"
                 if http_ok(f"{ICEBERG_REST_URL}/v1/config") else "offline"),
            chip("MinIO storage", http_ok(f"{MINIO_URL}/minio/health/live"), "healthy"
                 if http_ok(f"{MINIO_URL}/minio/health/live") else "offline"),
            chip("Trino engine", trino_up, "accepting queries" if trino_up else "unavailable"),
        ],
        "job_name": job_name,
    }


@app.get("/api/overview")
def overview():
    """KPI tiles, per-minute series, device split and the live event tail."""
    try:
        totals = query(
            f"""
            SELECT count(*),
                   coalesce(sum(purchase_amount), 0),
                   coalesce(avg(purchase_amount), 0),
                   count(DISTINCT user_id),
                   count(purchase_amount)
            FROM {TABLE}
            """
        )[0]

        # One point per minute over the last 30 minutes of event time.
        series = query(
            f"""
            SELECT date_format(minute, '%H:%i') AS label,
                   events,
                   revenue
            FROM (
              SELECT date_trunc('minute', event_time) AS minute,
                     count(*) AS events,
                     -- purchase_amount is nullable in the sink (Flink filters on
                     -- event_type only), so a minute can sum to NULL.
                     coalesce(sum(purchase_amount), 0) AS revenue
              FROM {TABLE}
              GROUP BY 1
              ORDER BY 1 DESC
              LIMIT 30
            )
            ORDER BY minute ASC
            """
        )

        devices = query(
            f"""
            SELECT device, count(*), coalesce(sum(purchase_amount), 0)
            FROM {TABLE}
            GROUP BY 1
            ORDER BY 3 DESC
            """
        )

        recent = query(
            f"""
            SELECT date_format(event_time, '%H:%i:%s') AS at,
                   device,
                   url,
                   purchase_amount,
                   user_id
            FROM {TABLE}
            ORDER BY event_time DESC
            LIMIT 12
            """
        )

        return {
            "kpis": {
                "purchases": int(totals[0]),
                "revenue": float(totals[1]),
                "avg_order": float(totals[2]),
                "users": int(totals[3]),
                # The generator only attaches an amount to ~30% of purchases, so
                # the tile states the priced share rather than leaving the table's
                # blank amounts unexplained.
                "priced": int(totals[4]),
            },
            "series": [{"label": r[0], "events": int(r[1]), "revenue": float(r[2] or 0)} for r in series],
            "devices": [{"device": r[0] or "unknown", "events": int(r[1]), "revenue": float(r[2] or 0)} for r in devices],
            "recent": [
                {
                    "at": r[0],
                    "device": r[1],
                    "url": r[2],
                    "amount": float(r[3]) if r[3] is not None else None,
                    "user": (r[4] or "")[:8],
                }
                for r in recent
            ],
        }
    except Exception as exc:
        # The sink table only exists once Flink's CTAS has run; say so plainly
        # instead of surfacing a raw stack trace to the page.
        return JSONResponse(
            status_code=503,
            content={"error": "pipeline_not_ready", "detail": str(exc)[:400]},
        )


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")
