import argparse
import csv
import json
import os
import resource
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    LogisticRegression,
    NaiveBayes,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PARQUET_DIR = Path(os.environ.get("PARQUET_OUT_DIR", "../data/preprocessed"))
LOG_DIR = Path(os.environ.get("LOG_DIR", "../logs"))
TABLEAU_DIR = Path(os.environ.get("TABLEAU_DIR", "../data/tableau_exports"))
RANDOM_SEED = int(os.environ.get("RANDOM_SEED", 42))


FEATURE_COLS = [
    "marital_status",
    "gender",
    "age_at_enrollment",
    "international",
    "displaced",
    "educational_special_needs",
    "nacionality",
    "mothers_qualification",
    "fathers_qualification",
    "mothers_occupation",
    "fathers_occupation",
    "scholarship_holder",
    "debtor",
    "tuition_fees_up_to_date",
    "application_mode",
    "application_order",
    "course",
    "daytime_evening_attendance",
    "previous_qualification",
    "previous_qualification_grade",
    "admission_grade",
    "curricular_units_1st_sem_credited",
    "curricular_units_1st_sem_enrolled",
    "curricular_units_1st_sem_evaluations",
    "curricular_units_1st_sem_approved",
    "curricular_units_1st_sem_grade",
    "curricular_units_2nd_sem_credited",
    "curricular_units_2nd_sem_enrolled",
    "curricular_units_2nd_sem_evaluations",
    "curricular_units_2nd_sem_approved",
    "curricular_units_2nd_sem_grade",
    "unemployment_rate",
    "inflation_rate",
    "gdp",
    "pass_rate_1st_sem",
    "pass_rate_2nd_sem",
    "grade_delta",
    "financial_stability_index",
]


@dataclass(frozen=True)
class MetricCriterion:
    key: str
    label: str
    target: float | None = None
    operator: str | None = None
    unit: str = ""

    def is_passed(self, value: float) -> bool | None:
        if self.target is None or self.operator is None:
            return None

        if self.operator == ">":
            return value > self.target
        if self.operator == ">=":
            return value >= self.target
        if self.operator == "<":
            return value < self.target
        if self.operator == "<=":
            return value <= self.target

        raise ValueError(f"Unsupported operator: {self.operator}")

    def target_text(self) -> str:
        if self.target is None or self.operator is None:
            return "Reference only"

        unit = f" {self.unit}" if self.unit else ""
        return f"Target {self.operator} {self.target:g}{unit}"


RF_QUALITY_CRITERIA = [
    MetricCriterion("accuracy", "Accuracy", 0.80, ">", ""),
    MetricCriterion("precision", "Precision", 0.75, ">", ""),
    MetricCriterion("recall", "Recall", 0.80, ">", ""),
    MetricCriterion("f1_score", "F1-Score", 0.77, ">", ""),
]


SYSTEM_PERFORMANCE_CRITERIA = [
    MetricCriterion("throughput", "Throughput", 500.0, ">", "rec/s"),
    MetricCriterion("p50_latency_ms", "P50 Latency", 100.0, "<", "ms"),
    MetricCriterion("e2e_latency_s", "End-to-End Latency",
                    30.0, "<", "seconds"),

    # Reference-only metric. Tidak diberi PASS/FAIL karena biasanya dibandingkan antar scale/run.
    MetricCriterion("training_time_s", "Training Time", None, None, "seconds"),
]


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("DropoutModelTraining_Case2_Rust")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.debug.maxToStringFields", "200")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_parquet(spark: SparkSession, parquet_dir: Path) -> DataFrame:
    if not parquet_dir.exists():
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")

    return spark.read.parquet(str(parquet_dir)).filter(F.col("label") >= 0)


def build_feature_pipeline(df: DataFrame) -> tuple[list[Any], list[str]]:
    available = set(df.columns)
    used_cols = [col for col in FEATURE_COLS if col in available]

    assembler = VectorAssembler(
        inputCols=used_cols,
        outputCol="features_raw",
        handleInvalid="skip",
    )
    scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")

    return [assembler, scaler], used_cols


def evaluate_model(
    model: Any,
    test_df: DataFrame,
    model_name: str,
) -> tuple[dict[str, Any], DataFrame]:
    predictions = model.transform(test_df)

    evaluators = {
        "accuracy": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy",
        ),
        "precision": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedPrecision",
        ),
        "recall": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="weightedRecall",
        ),
        "f1_score": MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="f1",
        ),
    }

    result = {"model": model_name}
    result.update(
        {
            metric_name: round(evaluator.evaluate(predictions), 4)
            for metric_name, evaluator in evaluators.items()
        }
    )

    return result, predictions


def get_rss_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)

    if sys.platform == "darwin":
        return usage.ru_maxrss / (1024 * 1024)

    return usage.ru_maxrss / 1024


def train_all_models(df: DataFrame) -> tuple[list[dict[str, Any]], list[tuple[str, float]], Any]:
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=RANDOM_SEED)

    total = train_df.count()
    counts = {
        row["label"]: row["count"]
        for row in train_df.groupBy("label").count().collect()
    }
    weights = {
        label: total / (3 * count)
        for label, count in counts.items()
        if count > 0
    }

    train_wdf = train_df.withColumn(
        "classWeight",
        F.when(F.col("label") == 0, weights.get(0, 1.0))
        .when(F.col("label") == 1, weights.get(1, 1.0))
        .when(F.col("label") == 2, weights.get(2, 1.0))
        .otherwise(1.0),
    )

    feat_stages, used_cols = build_feature_pipeline(df)
    results: list[dict[str, Any]] = []

    rss_before = get_rss_mb()
    t0 = time.time()

    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        weightCol="classWeight",
        numTrees=100,
        maxDepth=10,
        featureSubsetStrategy="sqrt",
        minInstancesPerNode=5,
        seed=RANDOM_SEED,
    )

    rf_model = Pipeline(stages=feat_stages + [rf]).fit(train_wdf)
    best_model = rf_model

    train_time = time.time() - t0
    rss_after = get_rss_mb()

    result, _ = evaluate_model(rf_model, test_df, "Random Forest")
    result["training_time_s"] = round(train_time, 2)
    result["rss_delta_mb"] = round(rss_after - rss_before, 1)
    results.append(result)

    feat_imp = sorted(
        zip(used_cols, rf_model.stages[-1].featureImportances.toArray()),
        key=lambda item: item[1],
        reverse=True,
    )

    model_specs = [
        (
            "Logistic Regression",
            LogisticRegression(
                labelCol="label",
                featuresCol="features",
                weightCol="classWeight",
                maxIter=100,
                family="multinomial",
            ),
            train_wdf,
        ),
        (
            "Decision Tree",
            DecisionTreeClassifier(
                labelCol="label",
                featuresCol="features",
                maxDepth=10,
                seed=RANDOM_SEED,
            ),
            train_df,
        ),
        (
            "Naive Bayes",
            NaiveBayes(
                labelCol="label",
                featuresCol="features",
                modelType="gaussian",
            ),
            train_df,
        ),
    ]

    for model_name, estimator, training_df in model_specs:
        t0 = time.time()
        model = Pipeline(stages=feat_stages + [estimator]).fit(training_df)
        train_time = time.time() - t0

        result, _ = evaluate_model(model, test_df, model_name)
        result["training_time_s"] = round(train_time, 2)
        results.append(result)

    return results, feat_imp, best_model


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def save_results(
    results: list[dict[str, Any]],
    feat_imp: list[tuple[str, float]],
    parquet_dir: Path,
) -> dict[str, Path]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    feature_rows = [
        {
            "feature": feature,
            "importance": round(importance, 4),
        }
        for feature, importance in feat_imp[:20]
    ]

    out = {
        "impl": "rust",
        "timestamp": datetime.now(UTC).isoformat(),
        "parquet_source": str(parquet_dir),
        "model_comparison": results,
        "feature_importance": feature_rows,
    }

    paths = {
        "json": LOG_DIR / f"training_results_rust_{ts}.json",
        "metrics_csv": LOG_DIR / f"model_metrics_rust_{ts}.csv",
        "features_csv": LOG_DIR / f"feature_importance_rust_{ts}.csv",
    }

    with paths["json"].open("w", encoding="utf-8") as file:
        json.dump(out, file, indent=2)

    write_csv(
        paths["metrics_csv"],
        results,
        fieldnames=[
            "model",
            "accuracy",
            "precision",
            "recall",
            "f1_score",
            "training_time_s",
            "rss_delta_mb",
        ],
    )

    write_csv(
        paths["features_csv"],
        feature_rows,
        fieldnames=["feature", "importance"],
    )

    return paths


def build_metric_rows(
    values: dict[str, float],
    criteria: list[MetricCriterion],
) -> list[dict[str, Any]]:
    rows = []

    for criterion in criteria:
        value = values.get(criterion.key)
        passed = criterion.is_passed(value) if value is not None else None

        rows.append(
            {
                "metric": criterion.key,
                "label": criterion.label,
                "value": round(value, 4) if value is not None else None,
                "unit": criterion.unit,
                "operator": criterion.operator or "",
                "target": criterion.target,
                "passed": passed,
            }
        )

    return rows


def save_metric_summary(name: str, rows: list[dict[str, Any]]) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    path = LOG_DIR / f"{name}_rust_{ts}.csv"

    write_csv(
        path,
        rows,
        fieldnames=[
            "metric",
            "label",
            "value",
            "unit",
            "operator",
            "target",
            "passed",
        ],
    )

    return path


def status(passed: bool | None) -> str:
    if passed is None:
        return "[INFO]"

    return "[PASS]" if passed else "[FAIL]"


def print_model_comparison(results: list[dict[str, Any]]) -> None:
    print("\n" + "-" * 80)
    print(
        f"{'Model':<22} "
        f"{'Accuracy':>9} "
        f"{'Precision':>10} "
        f"{'Recall':>8} "
        f"{'F1':>9} "
        f"{'Time(s)':>8}"
    )
    print("-" * 80)

    for result in results:
        print(
            f"{result['model']:<22} "
            f"{result['accuracy']:>9.4f} "
            f"{result['precision']:>10.4f} "
            f"{result['recall']:>8.4f} "
            f"{result['f1_score']:>9.4f} "
            f"{result['training_time_s']:>8.2f}"
        )

    print("-" * 80)


def print_rf_quality_summary(rf_results: dict[str, Any]) -> Path:
    print("\n" + "=" * 80)
    print(" MODEL EVALUATION (RANDOM FOREST)")
    print("=" * 80)

    metric_values = {
        "accuracy": float(rf_results["accuracy"]),
        "precision": float(rf_results["precision"]),
        "recall": float(rf_results["recall"]),
        "f1_score": float(rf_results["f1_score"]),
    }

    metric_rows = build_metric_rows(metric_values, RF_QUALITY_CRITERIA)

    for row in metric_rows:
        criterion = next(
            item for item in RF_QUALITY_CRITERIA
            if item.key == row["metric"]
        )
        value = row["value"]
        unit = f" {row['unit']}" if row["unit"] else ""

        print(
            f" {row['label']:<16} : {value:.4f}{unit} "
            f"({criterion.target_text()}) -> {status(row['passed'])}"
        )

    csv_path = save_metric_summary("rf_quality_summary", metric_rows)
    print(f"\n[CSV] RF quality summary saved: {csv_path}")

    pass_fail_rows = [row for row in metric_rows if row["passed"] is not None]

    if all(row["passed"] for row in pass_fail_rows):
        print(
            "\n[CONCLUSION] Model MEETS all institutional prediction quality standards."
        )
    else:
        print(
            "\n[CONCLUSION] Model DOES NOT MEET standards. "
            "Further tuning or data required."
        )

    print("=" * 80)
    return csv_path


def read_latest_rust_producer_log() -> dict[str, Any] | None:
    log_files = [
        path
        for path in LOG_DIR.glob("producer_rust*.json")
        if path.is_file()
    ]

    if not log_files:
        return None

    latest_log = max(log_files, key=lambda path: path.stat().st_ctime)

    with latest_log.open("r", encoding="utf-8") as file:
        return json.load(file)


def calculate_e2e_latency_s(final_predictions: DataFrame) -> float | None:
    current_time_ms = int(time.time() * 1000)

    latency_df = final_predictions.filter(F.col("event_ts_ms") > 0).withColumn(
        "e2e_latency_ms",
        F.lit(current_time_ms) - F.col("event_ts_ms").cast("long"),
    )

    summary = latency_df.agg(
        F.count("*").alias("count"),
        F.avg("e2e_latency_ms").alias("avg_e2e_ms"),
    ).collect()[0]

    if summary["count"] == 0 or summary["avg_e2e_ms"] is None:
        return None

    return float(summary["avg_e2e_ms"]) / 1000.0


def print_system_performance(
    rf_results: dict[str, Any],
    final_predictions: DataFrame,
) -> Path:
    print("\n" + "=" * 80)
    print(" SYSTEM PERFORMANCE EVALUATION")
    print("=" * 80)

    rust_data = read_latest_rust_producer_log()

    metric_values: dict[str, float] = {
        "training_time_s": float(rf_results.get("training_time_s", 0.0)),
    }

    if rust_data is not None:
        metric_values["throughput"] = float(rust_data.get("avg_rate", 0.0))
        metric_values["p50_latency_ms"] = (
            float(rust_data.get("latency_us", {}).get("p50", 0.0)) / 1000.0
        )

    e2e_latency_s = calculate_e2e_latency_s(final_predictions)
    if e2e_latency_s is not None:
        metric_values["e2e_latency_s"] = e2e_latency_s

    metric_rows = build_metric_rows(metric_values, SYSTEM_PERFORMANCE_CRITERIA)

    for row in metric_rows:
        criterion = next(
            item for item in SYSTEM_PERFORMANCE_CRITERIA
            if item.key == row["metric"]
        )

        if row["value"] is None:
            print(f" {row['label']:<18}: Data unavailable -> {status(None)}")
            continue

        unit = f" {row['unit']}" if row["unit"] else ""

        print(
            f" {row['label']:<18}: {row['value']:.2f}{unit} "
            f"({criterion.target_text()}) -> {status(row['passed'])}"
        )

    csv_path = save_metric_summary("system_performance_summary", metric_rows)
    print(f"\n[CSV] System performance summary saved: {csv_path}")

    print("=" * 80)
    return csv_path


def export_tableau_predictions(final_predictions: DataFrame) -> Path:
    print("\n[Tableau Export] Saving prediction results to CSV...")
    TABLEAU_DIR.mkdir(parents=True, exist_ok=True)

    tableau_df = final_predictions.select(
        "marital_status",
        "gender",
        "age_at_enrollment",
        "financial_stability_index",
        "grade_delta",
        "target",
        "prediction",
        "label",
    )

    out_path = TABLEAU_DIR / "ml_dropout_predictions.csv"
    tableau_df.toPandas().to_csv(out_path, index=False)

    print(f"[SUCCESS] Predictions saved for dashboard: {out_path}")
    return out_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", type=Path, default=PARQUET_DIR)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print("=" * 80)
    print("CASE 2 — STAGE 3: TRAINING (Reading Parquet from consumer)")
    print("=" * 80)

    spark = create_spark_session()

    try:
        df = load_parquet(spark, args.parquet)

        results, feat_imp, best_model = train_all_models(df)
        print_model_comparison(results)

        result_paths = save_results(results, feat_imp, args.parquet)
        print(f"[JSON] Training results saved: {result_paths['json']}")
        print(f"[CSV] Model metrics saved: {result_paths['metrics_csv']}")
        print(f"[CSV] Feature importance saved: {
              result_paths['features_csv']}")

        final_predictions = best_model.transform(df)

        rf_results = results[0]
        print_rf_quality_summary(rf_results)
        print_system_performance(rf_results, final_predictions)
        export_tableau_predictions(final_predictions)

        print("\n[OK] Done. Proceed to: Stage 4 Visualizer")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
