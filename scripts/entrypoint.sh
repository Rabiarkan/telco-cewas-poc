#!/bin/bash

# CEWAS — Pipeline Runner (Phase 1: ETL + Feature Store)
# ingest → feature store
# todo: + train → scoring 

set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]  $*"; }
err() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

cd /home/jovyan/work

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
log "=========================================="