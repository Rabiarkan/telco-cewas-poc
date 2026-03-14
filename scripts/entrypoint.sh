#!/bin/bash

# CEWAS — Pipeline Runner (Phase 1: ETL + Feature Store)
# ingest → feature store
# todo: + train → scoring 
wait_for_mlflow() {
  local url="http://mlflow:5000/health"
  local retries=20
  log "Waiting for MLflow at ${url}..."
  for i in $(seq 1 $retries); do
    if curl -sf "$url" > /dev/null 2>&1; then
      log "MLflow is ready."
      return 0
    fi
    log "  Attempt $i/$retries — retrying in 3s..."
    sleep 3
  done
  err "MLflow did not become ready in time."
  exit 1
}

set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*"; }
err() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

cd /home/jovyan/work
wait_for_mlflow

log "=========================================="
log "  CEWAS Pipeline START"
log "=========================================="

# ── Step 1: Raw CSV → Data Warehouse ──────────────────────────────────────────
log "Step 1/2 — Ingesting raw data to DW..."
python -m src.etl.ingest_to_dw
log "Step 1/2 — Done ✓"

# ── Step 2: DW → Feature Store ────────────────────────────────────────────────
log "Step 2/2 — Building feature store..."
python -m src.features.build_feature_store
log "Step 2/2 — Done ✓"

log "=========================================="
log "  CEWAS Pipeline COMPLETE"
log "Container staying alive for inspection."
log "Run: docker compose exec app bash — to enter"
exec tail -f /dev/null   
log "=========================================="