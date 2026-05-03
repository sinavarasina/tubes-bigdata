.PHONY: help prepare-data build-case2 run-case2 clean-case2

# ENV
export WORKSPACE_DIR   := /workspace
export DATASET_PATH    := $(WORKSPACE_DIR)/data/dataset_clean.csv
export PARQUET_OUT_DIR := $(WORKSPACE_DIR)/data/preprocessed
export LOG_DIR         := $(WORKSPACE_DIR)/logs
export TABLEAU_DIR     := $(WORKSPACE_DIR)/data/tableau_exports

# Execution variables
export RATE        ?= 0
export RANDOM_SEED ?= 42

# Internal paths
CASE2_RUST_DIR := case_2/dropout-bigdata-pipeline-rust
CASE2_PY_DIR   := case_2

help:
	@echo ""
	@echo "  tubes-bigdata — Dropout Prediction Pipeline"
	@echo "  ========================================================"
	@echo "  1. make prepare-data   : Download & extract dataset from UCI"
	@echo "  2. make build-case2    : Compile Rust binary (Release mode)"
	@echo "  3. make run-case2      : Run Full Pipeline"
	@echo "  4. make clean-case2    : Clean build artifacts & remove data"
	@echo ""
	@echo "  * You can override variables: make run-case2 RATE=500"
	@echo ""

prepare-data:
	@echo "[data] Downloading and preparing dataset..."
	python3 scripts/00_download_dataset.py

build-case2:
	@echo "[case2] Checking Rust toolchain..."
	@rustc --version || (echo "ERROR: Rust is not installed." && exit 1)
	@echo "[case2] Compiling Rust binary (Release)..."
	cd $(CASE2_RUST_DIR) && cargo build --release
	@echo "[case2] Build complete. Binary is ready."

run-case2: prepare-data build-case2
	@echo "\n======================================================="
	@echo "[case2] STAGE 1: Kafka Producer (Rust) (Rate: $(RATE) rec/s)"
	@echo "======================================================="
	cd $(CASE2_RUST_DIR) && ./target/release/producer --rate $(RATE)

	@echo "\n======================================================="
	@echo "[case2] STAGE 2: Kafka Consumer (Rust) -> Parquet"
	@echo "======================================================="
	cd $(CASE2_RUST_DIR) && ./target/release/consumer

	@echo "\n======================================================="
	@echo "[case2] STAGE 3: PySpark MLlib Training (Read Parquet)"
	@echo "======================================================="
	cd $(CASE2_PY_DIR) && python3 spark_training_parquet.py

	@echo "\n======================================================="
	@echo "[case2] STAGE 4: Throughput Visualization (Terminal/Plotters)"
	@echo "======================================================="
	cd $(CASE2_RUST_DIR) && ./target/release/visualizer

	@echo "\n[case2] Pipeline completed! Check logs/ and data/preprocessed/ folders."

clean-case2:
	@echo "[case2] Cleaning artifacts..."
	cd $(CASE2_RUST_DIR) && cargo clean
	rm -rf $(LOG_DIR)/producer_*.json
	rm -rf $(LOG_DIR)/training_*.json
	rm -rf $(LOG_DIR)/*.svg
	rm -rf $(PARQUET_OUT_DIR)
	@echo "[case2] Clean complete."
