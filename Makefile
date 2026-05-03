# ============================================================
# Master Makefile — Dropout Pipeline
# ============================================================

.PHONY: help prepare-data build-case2 run-case1 run-case2 benchmark clean

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
export N_RUNS                  ?= 3
export KAFKA_BOOTSTRAP_SERVERS := 127.0.0.1:9092

CASE1_DIR      := $(WORKSPACE_DIR)/case_1/dropout-bigdata-pipeline
CASE2_RUST_DIR := $(WORKSPACE_DIR)/case_2/dropout-bigdata-pipeline-rust
CASE2_PY_DIR   := $(WORKSPACE_DIR)/case_2

help:
	@echo "======================================================="
	@echo " tubes-bigdata — Master Pipeline"
	@echo "======================================================="
	@echo " make benchmark  : Run comparative benchmark (Rust vs Python - WIP)"
	@echo " make run-case1  : Run Case 1 (Python) end-to-end [WIP]"
	@echo " make run-case2  : Run Case 2 (Rust) end-to-end"
	@echo " make clean      : Remove all artifacts and logs"

prepare-data:
	@echo "[data] Checking/downloading dataset..."
	python3 scripts/00_download_dataset.py

build-case2:
	@echo "[case2] Compiling Rust binary (Release)..."
	cd $(CASE2_RUST_DIR) && cargo build --release

# ─────────────────────────────────────────────────────────────
# INDIVIDUAL EXECUTION
# ─────────────────────────────────────────────────────────────
run-case1: prepare-data
	@echo "\n=== CASE 1: Python Pipeline (WIP) ==="
	@echo "[INFO] Case 1 is currently under maintenance. Skipping execution."
	# cd $(CASE1_DIR) && python3 scripts/02_kafka_consumer_processing.py --idle-timeout 10 & \
	# sleep 2 && \
	# cd $(CASE1_DIR) && python3 scripts/01_kafka_producer.py --rate $(RATE) && \
	# wait
	# cd $(CASE1_DIR) && python3 spark/03_model_training.py
	# cd $(CASE1_DIR) && python3 scripts/04_visualize_results.py

run-case2: prepare-data build-case2
	@echo "\n=== CASE 2: Rust + PySpark Pipeline ==="
	@echo "[1] Starting Consumer (background) & Producer (Rate: $(RATE), Scale: $(SCALE)x)..."
	cd $(CASE2_RUST_DIR) && ./target/release/consumer --idle-timeout $(IDLE_TIMEOUT) & \
	sleep 2 && \
	cd $(CASE2_RUST_DIR) && ./target/release/producer --rate $(RATE) --iterations $(SCALE) && \
	wait
	@echo "[2] Training PySpark MLlib..."
	cd $(CASE2_PY_DIR) && python3 spark_training_parquet.py
	@echo "[3] Generating Plotters Visualization..."
	cd $(CASE2_RUST_DIR) && ./target/release/visualizer

# ─────────────────────────────────────────────────────────────
# COMPARATIVE BENCHMARK
# ─────────────────────────────────────────────────────────────
benchmark: prepare-data build-case2
	@echo "\n======================================================="
	@echo " STARTING COMPARATIVE BENCHMARK ($(N_RUNS) RUNS)"
	@echo "======================================================="
	
	@echo "\n[Phase 1/3: Python / Case 1] - WIP (Skipped)"
	# @for i in $$(seq 1 $(N_RUNS)); do \
	# 	echo "\n--- Python Run $$i/$(N_RUNS) ---"; \
	# 	rm -rf $(PARQUET_OUT_DIR)/*; \
	# 	cd $(CASE1_DIR) && python3 scripts/02_kafka_consumer_processing.py --idle-timeout $(IDLE_TIMEOUT) & \
	# 	sleep 2 && \
	# 	cd $(CASE1_DIR) && python3 scripts/01_kafka_producer.py --rate $(RATE) && \
	# 	wait; \
	# 	cd $(CASE1_DIR) && python3 spark/03_model_training.py; \
	# done

	@echo "\n[Phase 2/3: Rust / Case 2]"
	@for i in $$(seq 1 $(N_RUNS)); do \
		echo "\n--- Rust Run $$i/$(N_RUNS) ---"; \
		rm -rf $(PARQUET_OUT_DIR)/*; \
		GROUP_ID=bench-$$i-$$(date +%s); \
		cd $(CASE2_RUST_DIR) && ./target/release/consumer --group-id $$GROUP_ID --idle-timeout $(IDLE_TIMEOUT) & \
		sleep 4 && \
		cd $(CASE2_RUST_DIR) && ./target/release/producer --rate $(RATE) && \
		wait; \
		cd $(CASE2_PY_DIR) && python3 spark_training_parquet.py; \
	done

	@echo "\n[Phase 3/3: Statistical Calculation & Reporting]"
	python3 scripts/benchmark_comparative.py --idle-timeout 5

case-2-eval-scalability: prepare-data build-case2
	@echo "\n======================================================="
	@echo " STARTING END-TO-END SCALABILITY EVALUATION"
	@echo "======================================================="
	
	@echo "\n>>> [SCALE 1x] ~4,424 Records <<<"
	rm -rf $(PARQUET_OUT_DIR)/*
	cd $(CASE2_RUST_DIR) && ./target/release/consumer --idle-timeout $(IDLE_TIMEOUT) & \
	sleep 2 && \
	cd $(CASE2_RUST_DIR) && ./target/release/producer --rate $(RATE) --iterations 1 && \
	wait
	cd $(CASE2_PY_DIR) && python3 spark_training_parquet.py

	@echo "\n>>> [SCALE 5x] ~22,120 Records <<<"
	rm -rf $(PARQUET_OUT_DIR)/*
	cd $(CASE2_RUST_DIR) && ./target/release/consumer --idle-timeout $(IDLE_TIMEOUT) & \
	sleep 2 && \
	cd $(CASE2_RUST_DIR) && ./target/release/producer --rate $(RATE) --iterations 5 && \
	wait
	cd $(CASE2_PY_DIR) && python3 spark_training_parquet.py

	@echo "\n>>> [SCALE 10x] ~44,240 Records <<<"
	rm -rf $(PARQUET_OUT_DIR)/*
	cd $(CASE2_RUST_DIR) && ./target/release/consumer --idle-timeout $(IDLE_TIMEOUT) & \
	sleep 2 && \
	cd $(CASE2_RUST_DIR) && ./target/release/producer --rate $(RATE) --iterations 10 && \
	wait
	cd $(CASE2_PY_DIR) && python3 spark_training_parquet.py
	
	@echo "\n[SUCCESS] Scalability evaluation complete. Check logs/ directory."

clean:
	@echo "Cleaning up all artifacts..."
	cd $(CASE2_RUST_DIR) && cargo clean
	rm -rf $(LOG_DIR)/*
	rm -rf $(PARQUET_OUT_DIR)
	@echo "Cleanup finished."
