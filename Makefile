# ============================================================
# Makefile — Dropout Big Data Pipeline
# Rust Producer/Consumer + Kafka + Parquet + PySpark
# ============================================================

SHELL := /bin/bash

.PHONY: \
	help \
	prepare-data \
	build \
	run \
	eval-scalability \
	clean

# ─────────────────────────────────────────────────────────────
# ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────────────────────

export WORKSPACE_DIR           := /workspace
export DATASET_PATH            := $(WORKSPACE_DIR)/data/dataset_clean.csv
export PARQUET_OUT_DIR         := $(WORKSPACE_DIR)/data/preprocessed
export LOG_DIR                 := $(WORKSPACE_DIR)/logs
export TABLEAU_DIR             := $(WORKSPACE_DIR)/data/tableau_exports

export IDLE_TIMEOUT            ?= 5
export RATE                    ?= 0
export SCALE                   ?= 1
export RANDOM_SEED             ?= 42
export LATENCY_MEASURE         ?= 0
export KAFKA_BOOTSTRAP_SERVERS ?= 127.0.0.1:9092

RUST_DIR := $(WORKSPACE_DIR)/src/dropout-bigdata-pipeline-rust
PY_DIR   := $(WORKSPACE_DIR)/src

# ─────────────────────────────────────────────────────────────
# HELP
# ─────────────────────────────────────────────────────────────

help:
	@echo "======================================================="
	@echo " tubes-bigdata — Rust + PySpark Pipeline"
	@echo "======================================================="
	@echo " make run              : Run full pipeline"
	@echo " make run RATE=500     : Run with producer rate limit"
	@echo " make run RATE=0       : Run in burst mode"
	@echo " make run SCALE=5      : Repeat dataset 5x"
	@echo " make eval-scalability : Run scale 1x, 5x, 10x"
	@echo " make clean            : Remove artifacts and logs"
	@echo "======================================================="
	@echo " Current config:"
	@echo "   KAFKA_BOOTSTRAP_SERVERS = $(KAFKA_BOOTSTRAP_SERVERS)"
	@echo "   RATE                    = $(RATE)"
	@echo "   SCALE                   = $(SCALE)"
	@echo "   IDLE_TIMEOUT            = $(IDLE_TIMEOUT)"
	@echo "======================================================="

# ─────────────────────────────────────────────────────────────
# PREPARE / BUILD
# ─────────────────────────────────────────────────────────────

prepare-data:
	@echo "[data] Checking/downloading dataset..."
	python3 scripts/00_download_dataset.py
	@mkdir -p "$(PARQUET_OUT_DIR)" "$(LOG_DIR)" "$(TABLEAU_DIR)"

build:
	@echo "[build] Compiling Rust binaries (release)..."
	cd "$(RUST_DIR)" && cargo build --release

# ─────────────────────────────────────────────────────────────
# RUN PIPELINE
# ─────────────────────────────────────────────────────────────

run: prepare-data build
	@echo "\n=== Rust + Kafka + Parquet + PySpark Pipeline ==="
	@echo "[0] Cleaning previous Parquet output..."
	rm -rf "$(PARQUET_OUT_DIR)"/*
	mkdir -p "$(PARQUET_OUT_DIR)"

	@echo "[1] Starting Consumer (background) & Producer (Rate: $(RATE), Scale: $(SCALE)x)..."
	cd "$(RUST_DIR)" && ./target/release/consumer --idle-timeout "$(IDLE_TIMEOUT)" & \
	sleep 2 && \
	cd "$(RUST_DIR)" && ./target/release/producer --rate "$(RATE)" --iterations "$(SCALE)" && \
	wait

	@echo "[2] Training / processing with PySpark..."
	cd "$(PY_DIR)" && python3 spark_training_parquet.py

	@echo "[3] Generating visualization..."
	cd "$(RUST_DIR)" && ./target/release/visualizer

	@echo "\n[SUCCESS] Pipeline finished."
	@echo "Logs       : $(LOG_DIR)"
	@echo "Parquet    : $(PARQUET_OUT_DIR)"
	@echo "Tableau CSV: $(TABLEAU_DIR)"

# ─────────────────────────────────────────────────────────────
# SCALABILITY EVALUATION
# ─────────────────────────────────────────────────────────────

eval-scalability: prepare-data build
	@echo "\n======================================================="
	@echo " STARTING END-TO-END SCALABILITY EVALUATION"
	@echo "======================================================="

	@echo "\n>>> [SCALE 1x] ~4,424 Records <<<"
	rm -rf "$(PARQUET_OUT_DIR)"/*
	mkdir -p "$(PARQUET_OUT_DIR)"
	cd "$(RUST_DIR)" && ./target/release/consumer --idle-timeout "$(IDLE_TIMEOUT)" & \
	sleep 2 && \
	cd "$(RUST_DIR)" && ./target/release/producer --rate "$(RATE)" --iterations 1 && \
	wait
	cd "$(PY_DIR)" && python3 spark_training_parquet.py

	@echo "\n>>> [SCALE 5x] ~22,120 Records <<<"
	rm -rf "$(PARQUET_OUT_DIR)"/*
	mkdir -p "$(PARQUET_OUT_DIR)"
	cd "$(RUST_DIR)" && ./target/release/consumer --idle-timeout "$(IDLE_TIMEOUT)" & \
	sleep 2 && \
	cd "$(RUST_DIR)" && ./target/release/producer --rate "$(RATE)" --iterations 5 && \
	wait
	cd "$(PY_DIR)" && python3 spark_training_parquet.py

	@echo "\n>>> [SCALE 10x] ~44,240 Records <<<"
	rm -rf "$(PARQUET_OUT_DIR)"/*
	mkdir -p "$(PARQUET_OUT_DIR)"
	cd "$(RUST_DIR)" && ./target/release/consumer --idle-timeout "$(IDLE_TIMEOUT)" & \
	sleep 2 && \
	cd "$(RUST_DIR)" && ./target/release/producer --rate "$(RATE)" --iterations 10 && \
	wait
	cd "$(PY_DIR)" && python3 spark_training_parquet.py

	@echo "\n[visualizer] Generating Plotters visualization..."
	cd "$(RUST_DIR)" && ./target/release/visualizer

	@echo "\n[SUCCESS] Scalability evaluation complete."
	@echo "Check logs directory: $(LOG_DIR)"

# ─────────────────────────────────────────────────────────────
# CLEAN
# ─────────────────────────────────────────────────────────────

clean:
	@echo "[clean] Removing build artifacts, logs, parquet, and tableau exports..."
	cd "$(RUST_DIR)" && cargo clean
	rm -rf "$(LOG_DIR)"/*
	rm -rf "$(PARQUET_OUT_DIR)"
	rm -rf "$(TABLEAU_DIR)"/*
	@echo "[clean] Done."
