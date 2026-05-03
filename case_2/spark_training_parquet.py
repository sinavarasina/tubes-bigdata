import os
import sys
import json
import time
import argparse
import resource
from datetime import datetime, UTC

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
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator,
)

# ─────────────────────────────────────────────────
PARQUET_DIR = os.environ.get("PARQUET_OUT_DIR", "../data/preprocessed")
LOG_DIR = os.environ.get("LOG_DIR", "../logs")
TABLEAU_DIR = os.environ.get("TABLEAU_DIR", "../data/tableau_exports")
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


def create_spark_session():
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


def load_parquet(spark, parquet_dir):
    if not os.path.exists(parquet_dir):
        print(f"[ERROR] Parquet directory not found: {parquet_dir}")
        print("        Run first: cargo run --release --bin consumer")
        sys.exit(1)

    df = spark.read.parquet(parquet_dir)
    df = df.filter(F.col("label") >= 0)
    print(f"[OK] Parquet loaded from: {parquet_dir} ({df.count():,} records)")
    return df


def build_feature_pipeline(df):
    available = set(df.columns)
    used_cols = [c for c in FEATURE_COLS if c in available]
    missing = [c for c in FEATURE_COLS if c not in available]
    if missing:
        print(f"[WARN] Missing columns: {missing}")

    assembler = VectorAssembler(
        inputCols=used_cols, outputCol="features_raw", handleInvalid="skip"
    )
    scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")
    return [assembler, scaler], used_cols


def evaluate_model(model, test_df, model_name):
    predictions = model.transform(test_df)

    ev_acc = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
    ev_f1 = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1")
    ev_prec = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    ev_rec = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedRecall")

    return {
        "model": model_name,
        "accuracy": round(ev_acc.evaluate(predictions), 4),
        "precision": round(ev_prec.evaluate(predictions), 4),
        "recall": round(ev_rec.evaluate(predictions), 4),
        "f1_score": round(ev_f1.evaluate(predictions), 4),
    }, predictions


def get_rss_mb():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == "darwin":
        return usage.ru_maxrss / (1024 * 1024)
    return usage.ru_maxrss / 1024


def train_all_models(spark, df):
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=RANDOM_SEED)
    print(f"[INFO] Train: {train_df.count():,} | Test: {test_df.count():,}")

    total = train_df.count()
    counts = {r["label"]: r["count"]
              for r in train_df.groupBy("label").count().collect()}
    weights = {k: total / (3 * v) for k, v in counts.items()}

    train_wdf = train_df.withColumn(
        "classWeight",
        F.when(F.col("label") == 0, weights.get(0, 1.0))
         .when(F.col("label") == 1, weights.get(1, 1.0))
         .when(F.col("label") == 2, weights.get(2, 1.0))
         .otherwise(1.0)
    )

    feat_stages, used_cols = build_feature_pipeline(df)
    results = []

    best_model = None

    print("\n[Training] Random Forest...")
    rss_before = get_rss_mb()
    t0 = time.time()

    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features", weightCol="classWeight",
        numTrees=100, maxDepth=10, featureSubsetStrategy="sqrt",
        minInstancesPerNode=5, seed=RANDOM_SEED
    )

    rf_model = Pipeline(stages=feat_stages + [rf]).fit(train_wdf)
    best_model = rf_model  # primary model

    train_time = time.time() - t0
    rss_after = get_rss_mb()

    r, _ = evaluate_model(rf_model, test_df, "Random Forest")
    r["training_time_s"] = round(train_time, 2)
    r["rss_delta_mb"] = round(rss_after - rss_before, 1)
    results.append(r)

    print(f"  Training time: {train_time:.2f}s | RSS delta: {
          r['rss_delta_mb']} MB")

    feat_imp = sorted(
        zip(used_cols, rf_model.stages[-1].featureImportances.toArray()),
        key=lambda x: x[1], reverse=True
    )

    print("\n[Training] Logistic Regression (baseline)...")
    t0 = time.time()

    lr = LogisticRegression(
        labelCol="label", featuresCol="features", weightCol="classWeight",
        maxIter=100, family="multinomial"
    )

    lr_model = Pipeline(stages=feat_stages + [lr]).fit(train_wdf)
    train_time = time.time() - t0

    r, _ = evaluate_model(lr_model, test_df, "Logistic Regression")
    r["training_time_s"] = round(train_time, 2)
    results.append(r)

    print("\n[Training] Decision Tree (baseline)...")
    t0 = time.time()

    dt = DecisionTreeClassifier(
        labelCol="label", featuresCol="features", maxDepth=10, seed=RANDOM_SEED
    )

    dt_model = Pipeline(stages=feat_stages + [dt]).fit(train_df)
    train_time = time.time() - t0

    r, _ = evaluate_model(dt_model, test_df, "Decision Tree")
    r["training_time_s"] = round(train_time, 2)
    results.append(r)

    print("\n[Training] Naive Bayes (baseline)...")
    t0 = time.time()

    nb = NaiveBayes(labelCol="label", featuresCol="features",
                    modelType="gaussian")

    nb_model = Pipeline(stages=feat_stages + [nb]).fit(train_df)
    train_time = time.time() - t0

    r, _ = evaluate_model(nb_model, test_df, "Naive Bayes")
    r["training_time_s"] = round(train_time, 2)
    results.append(r)

    return results, feat_imp, best_model


def save_results(results, feat_imp, parquet_dir):
    os.makedirs(LOG_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    out = {
        "impl": "rust",
        "timestamp": datetime.now(UTC).isoformat(),
        "parquet_source": parquet_dir,
        "model_comparison": results,
        "feature_importance": [
            {"feature": f, "importance": round(i, 4)}
            for f, i in feat_imp[:20]
        ],
    }

    path = os.path.join(LOG_DIR, f"training_results_rust_{ts}.json")

    with open(path, "w") as f:
        json.dump(out, f, indent=2)

    print(f"\n[OK] Results saved: {path}")
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", default=PARQUET_DIR)
    args = parser.parse_args()

    print("=" * 60)
    print("CASE 2 — STAGE 3: TRAINING (reading Parquet from Rust consumer)")
    print("=" * 60)

    spark = create_spark_session()
    df = load_parquet(spark, args.parquet)

    results, feat_imp, best_model = train_all_models(spark, df)

    print("\n" + "=" * 80)
    print(f"{'Model':<22} {'Accuracy':>9} {'Precision':>10} {
          'Recall':>8} {'F1':>9} {'Time(s)':>8}")
    print("-" * 80)

    for r in results:
        print(f"{r['model']:<22} {r['accuracy']:>9.4f} {r['precision']:>10.4f} {
              r['recall']:>8.4f} {r['f1_score']:>9.4f} {r['training_time_s']:>8.2f}")

    print("=" * 80)

    save_results(results, feat_imp, args.parquet)

    print("\n[Tableau Export] Saving prediction results to CSV...")
    os.makedirs(TABLEAU_DIR, exist_ok=True)

    final_predictions = best_model.transform(df)

    tableau_df = final_predictions.select(
        "marital_status", "gender", "age_at_enrollment",
        "financial_stability_index", "grade_delta",
        "target", "prediction", "label"
    )

    out_path = os.path.join(TABLEAU_DIR, "ml_dropout_predictions.csv")
    tableau_df.toPandas().to_csv(out_path, index=False)

    print(f"[SUCCESS] Predictions saved for dashboard: {out_path}")

    spark.stop()
    print("\n[OK] Done. Proceed to: Stage 4 Visualizer (Terminal)")
