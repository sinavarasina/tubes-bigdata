#!/usr/bin/env python3
import argparse
import json
import os
import resource
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.classification import (
    RandomForestClassifier,
    LogisticRegression,
    DecisionTreeClassifier,
    NaiveBayes,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

PARQUET_DIR = os.environ.get(
    "PARQUET_OUT_DIR",
    os.path.join(os.path.dirname(__file__), "..",
                 "..", "data", "preprocessed"),
)
LOG_DIR = os.environ.get(
    "LOG_DIR",
    os.path.join(os.path.dirname(__file__), "..", "logs"),
)
TABLEAU_DIR = os.environ.get(
    "TABLEAU_DIR",
    os.path.join(os.path.dirname(__file__), "..",
                 "..", "data", "tableau_exports"),
)
RANDOM_SEED = int(os.environ.get("RANDOM_SEED", 42))

FEATURE_COLS = [
    "marital_status", "gender", "age_at_enrollment", "international",
    "displaced", "educational_special_needs", "nacionality",
    "mothers_qualification", "fathers_qualification",
    "mothers_occupation", "fathers_occupation",
    "scholarship_holder", "debtor", "tuition_fees_up_to_date",
    "application_mode", "application_order", "course",
    "daytime_evening_attendance", "previous_qualification",
    "previous_qualification_grade", "admission_grade",
    "curricular_units_1st_sem_credited", "curricular_units_1st_sem_enrolled",
    "curricular_units_1st_sem_evaluations", "curricular_units_1st_sem_approved",
    "curricular_units_1st_sem_grade",
    "curricular_units_2nd_sem_credited", "curricular_units_2nd_sem_enrolled",
    "curricular_units_2nd_sem_evaluations", "curricular_units_2nd_sem_approved",
    "curricular_units_2nd_sem_grade",
    "unemployment_rate", "inflation_rate", "gdp",
    "pass_rate_1st_sem", "pass_rate_2nd_sem",
    "grade_delta", "financial_stability_index",
]


def create_spark():
    spark = (
        SparkSession.builder
        .appName("DropoutTraining_Case1_Python")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory",   "2g")
        .config("spark.sql.adaptive.enabled",    "true")
        .config("spark.sql.shuffle.partitions",  "4")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_parquet(spark, parquet_dir):
    if not os.path.exists(parquet_dir) or not os.listdir(parquet_dir):
        print(f"[ERROR] Parquet not found or empty: {parquet_dir}")
        print("        Run stage 1+2 first.")
        sys.exit(1)
    df = spark.read.parquet(parquet_dir).filter(F.col("label") >= 0)
    print(f"[OK] Parquet loaded: {df.count():,} records from {parquet_dir}")
    return df


def build_pipeline(df):
    available = set(df.columns)
    used = [c for c in FEATURE_COLS if c in available]
    missing = [c for c in FEATURE_COLS if c not in available]
    if missing:
        print(f"[WARN] Missing columns (skipped): {missing}")
    asm = VectorAssembler(
        inputCols=used, outputCol="features_raw", handleInvalid="skip")
    scl = MinMaxScaler(inputCol="features_raw", outputCol="features")
    return [asm, scl], used


def evaluate(model, test_df, name):
    preds = model.transform(test_df)

    def ev(m): return MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName=m
    ).evaluate(preds)
    return {
        "model": name,
        "accuracy": round(ev("accuracy"), 4),
        "precision": round(ev("weightedPrecision"), 4),
        "recall": round(ev("weightedRecall"), 4),
        "f1_score": round(ev("f1"), 4),
    }, preds


def get_rss_mb():
    u = resource.getrusage(resource.RUSAGE_SELF)
    return u.ru_maxrss / (1024 * 1024) if sys.platform == "darwin" else u.ru_maxrss / 1024


def train(spark, df):
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=RANDOM_SEED)
    print(f"[INFO] Train: {train_df.count():,} | Test: {test_df.count():,}")

    total = train_df.count()
    counts = {r["label"]: r["count"]
              for r in train_df.groupBy("label").count().collect()}
    w = {k: total / (3 * v) for k, v in counts.items()}

    train_w = train_df.withColumn(
        "classWeight",
        F.when(F.col("label") == 0, w.get(0, 1.0))
         .when(F.col("label") == 1, w.get(1, 1.0))
         .when(F.col("label") == 2, w.get(2, 1.0))
         .otherwise(1.0)
    )

    stages, used = build_pipeline(df)
    results = []
    best_model = None

    # ── Random Forest ────────────────────────────────────────────
    print("\n[Training] Random Forest...")
    rss0 = get_rss_mb()
    t0 = time.time()
    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features", weightCol="classWeight",
        numTrees=100, maxDepth=10, featureSubsetStrategy="sqrt",
        minInstancesPerNode=5, seed=RANDOM_SEED,
    )
    rf_model = Pipeline(stages=stages + [rf]).fit(train_w)
    tt = time.time() - t0
    best_model = rf_model
    r, _ = evaluate(rf_model, test_df, "Random Forest")
    r["training_time_s"] = round(tt, 2)
    r["rss_delta_mb"] = round(get_rss_mb() - rss0, 1)
    results.append(r)
    print(f"  {tt:.2f}s | RSS Δ: {r['rss_delta_mb']} MB")

    feat_imp = sorted(
        zip(used, rf_model.stages[-1].featureImportances.toArray()),
        key=lambda x: x[1], reverse=True,
    )

    # ── Logistic Regression ──────────────────────────────────────
    print("\n[Training] Logistic Regression...")
    t0 = time.time()
    lr = LogisticRegression(
        labelCol="label", featuresCol="features", weightCol="classWeight",
        maxIter=100, family="multinomial",
    )
    lr_model = Pipeline(stages=stages + [lr]).fit(train_w)
    r, _ = evaluate(lr_model, test_df, "Logistic Regression")
    r["training_time_s"] = round(time.time() - t0, 2)
    results.append(r)

    # ── Decision Tree ────────────────────────────────────────────
    print("\n[Training] Decision Tree...")
    t0 = time.time()
    dt = DecisionTreeClassifier(
        labelCol="label", featuresCol="features", maxDepth=10, seed=RANDOM_SEED,
    )
    dt_model = Pipeline(stages=stages + [dt]).fit(train_df)
    r, _ = evaluate(dt_model, test_df, "Decision Tree")
    r["training_time_s"] = round(time.time() - t0, 2)
    results.append(r)

    # ── Naive Bayes (gaussian) ───────────────────────────────────
    print("\n[Training] Naive Bayes (gaussian)...")
    t0 = time.time()
    nb = NaiveBayes(labelCol="label", featuresCol="features",
                    modelType="gaussian")
    nb_model = Pipeline(stages=stages + [nb]).fit(train_df)
    r, _ = evaluate(nb_model, test_df, "Naive Bayes")
    r["training_time_s"] = round(time.time() - t0, 2)
    results.append(r)

    return results, feat_imp, best_model, test_df


def save_results(results, feat_imp, parquet_dir):
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = {
        "impl": "python",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "parquet_source": parquet_dir,
        "model_comparison": results,
        "feature_importance": [
            {"feature": f, "importance": round(i, 4)} for f, i in feat_imp[:20]
        ],
    }
    path = os.path.join(LOG_DIR, f"training_results_{
                        datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\n[OK] Results saved: {path}")
    return path


def export_tableau(best_model, df):
    os.makedirs(TABLEAU_DIR, exist_ok=True)
    preds = best_model.transform(df)
    sel = preds.select(
        "marital_status", "gender", "age_at_enrollment",
        "financial_stability_index", "grade_delta",
        "target", "prediction", "label",
    )
    out = os.path.join(TABLEAU_DIR, "case1_predictions.csv")
    sel.toPandas().to_csv(out, index=False)
    print(f"[OK] Tableau export: {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", default=PARQUET_DIR)
    args = parser.parse_args()

    print("=" * 60)
    print("CASE 1 — STAGE 3: TRAINING (Python / PySpark)")
    print("=" * 60)

    spark = create_spark()
    df = load_parquet(spark, args.parquet)
    results, feat_imp, best_model, test_df = train(spark, df)

    print("\n" + "=" * 80)
    print(f"{'Model':<22} {'Accuracy':>9} {'Precision':>10} {
          'Recall':>8} {'F1':>9} {'Time(s)':>8}")
    print("-" * 80)
    for r in results:
        print(f"{r['model']:<22} {r['accuracy']:>9.4f} {r['precision']:>10.4f} "
              f"{r['recall']:>8.4f} {r['f1_score']:>9.4f} {r['training_time_s']:>8.2f}")
    print("=" * 80)

    save_results(results, feat_imp, args.parquet)
    export_tableau(best_model, df)
    spark.stop()
    print("\n[OK] Done.")
