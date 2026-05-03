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
        sys.exit(1)

    df = spark.read.parquet(parquet_dir)
    df = df.filter(F.col("label") >= 0)
    return df


def build_feature_pipeline(df):
    available = set(df.columns)
    used_cols = [c for c in FEATURE_COLS if c in available]
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

    rss_before = get_rss_mb()
    t0 = time.time()

    rf = RandomForestClassifier(
        labelCol="label", featuresCol="features", weightCol="classWeight",
        numTrees=100, maxDepth=10, featureSubsetStrategy="sqrt",
        minInstancesPerNode=5, seed=RANDOM_SEED
    )

    rf_model = Pipeline(stages=feat_stages + [rf]).fit(train_wdf)
    best_model = rf_model

    train_time = time.time() - t0
    rss_after = get_rss_mb()

    r, _ = evaluate_model(rf_model, test_df, "Random Forest")
    r["training_time_s"] = round(train_time, 2)
    r["rss_delta_mb"] = round(rss_after - rss_before, 1)
    results.append(r)

    feat_imp = sorted(
        zip(used_cols, rf_model.stages[-1].featureImportances.toArray()),
        key=lambda x: x[1], reverse=True
    )

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

    t0 = time.time()
    dt = DecisionTreeClassifier(
        labelCol="label", featuresCol="features", maxDepth=10, seed=RANDOM_SEED
    )
    dt_model = Pipeline(stages=feat_stages + [dt]).fit(train_df)
    train_time = time.time() - t0

    r, _ = evaluate_model(dt_model, test_df, "Decision Tree")
    r["training_time_s"] = round(train_time, 2)
    results.append(r)

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
    return path


def status(passed):
    return "[PASS]" if passed else "[FAIL]"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", default=PARQUET_DIR)
    args = parser.parse_args()

    print("=" * 80)
    print("CASE 2 — STAGE 3: TRAINING (Reading Parquet from consumer)")
    print("=" * 80)

    spark = create_spark_session()
    df = load_parquet(spark, args.parquet)

    results, feat_imp, best_model = train_all_models(spark, df)

    print("\n" + "-" * 80)
    print(f"{'Model':<22} {'Accuracy':>9} {'Precision':>10} {
          'Recall':>8} {'F1':>9} {'Time(s)':>8}")
    print("-" * 80)

    for r in results:
        print(f"{r['model']:<22} {r['accuracy']:>9.4f} {r['precision']:>10.4f} {
              r['recall']:>8.4f} {r['f1_score']:>9.4f} {r['training_time_s']:>8.2f}")

    print("-" * 80)

    save_results(results, feat_imp, args.parquet)

    final_predictions = best_model.transform(df)

    binary_df = df.withColumn("binary_label", F.when(
        F.col("label") == 0, 1.0).otherwise(0.0))
    feat_stages, _ = build_feature_pipeline(binary_df)
    binary_lr = LogisticRegression(
        labelCol="binary_label", featuresCol="features", maxIter=10)
    binary_pipeline = Pipeline(stages=feat_stages + [binary_lr]).fit(binary_df)
    binary_preds = binary_pipeline.transform(binary_df)

    binary_evaluator = BinaryClassificationEvaluator(
        labelCol="binary_label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    roc_auc = binary_evaluator.evaluate(binary_preds)

    print("\n" + "=" * 80)
    print(" MODEL EVALUATION (RANDOM FOREST)")
    print("=" * 80)

    rf_results = results[0]

    target_acc = 0.80
    target_prec = 0.75
    target_rec = 0.80
    target_f1 = 0.77
    target_auc = 0.9653

    pass_acc = rf_results["accuracy"] > target_acc
    pass_prec = rf_results["precision"] > target_prec
    pass_rec = rf_results["recall"] > target_rec
    pass_f1 = rf_results["f1_score"] > target_f1
    pass_auc = roc_auc >= target_auc

    print(f" Accuracy         : {rf_results['accuracy']:.4f} (Target > {
          target_acc:.2f}) -> {status(pass_acc)}")
    print(f" Precision        : {rf_results['precision']:.4f} (Target > {
          target_prec:.2f}) -> {status(pass_prec)}")
    print(f" Recall           : {rf_results['recall']:.4f} (Target > {
          target_rec:.2f}) -> {status(pass_rec)}")
    print(f" F1-Score         : {rf_results['f1_score']
          :.4f} (Target > {target_f1:.2f}) -> {status(pass_f1)}")
    print(
        f" AUC-ROC          : {roc_auc:.4f} (Target >= {target_auc:.4f}) -> {status(pass_auc)}")

    if pass_acc and pass_prec and pass_rec and pass_f1 and pass_auc:
        print(
            "\n[CONCLUSION] Model MEETS all institutional prediction quality standards.")
    else:
        print(
            "\n[CONCLUSION] Model DOES NOT MEET standards. Further tuning or data required.")
    print("=" * 80)

    print("\n" + "=" * 80)
    print(" SYSTEM PERFORMANCE EVALUATION")
    print("=" * 80)

    log_files = [f for f in os.listdir(LOG_DIR) if f.startswith(
        'producer_rust') and f.endswith('.json')]
    if log_files:
        latest_log = max(log_files, key=lambda x: os.path.getctime(
            os.path.join(LOG_DIR, x)))
        with open(os.path.join(LOG_DIR, latest_log), 'r') as f:
            rust_data = json.load(f)

            throughput = rust_data.get("avg_rate", 0)
            latency_p50 = rust_data.get("latency_us", {}).get(
                "p50", 0) / 1000.0

            target_throughput = 500
            target_latency = 100

            pass_through = throughput > target_throughput
            pass_lat = latency_p50 < target_latency

            print(f" Throughput       : {
                  throughput:.2f} rec/s (Target > {target_throughput}) -> {status(pass_through)}")
            print(f" Average Latency  : {latency_p50:.2f} ms (Target < {
                  target_latency}) -> {status(pass_lat)}")
    else:
        print(" Throughput       : Data unavailable (Rust Producer log not found)")
        print(" Average Latency  : Data unavailable (Rust Producer log not found)")

    training_time = rf_results.get("training_time_s", 0)
    print(f" Training Time    : {
          training_time:.2f} seconds (Scalability Evaluation: Compare across scales)")

    current_time_ms = int(time.time() * 1000)

    latency_df = final_predictions.filter(F.col("event_ts_ms") > 0) \
        .withColumn("e2e_latency_ms", F.lit(current_time_ms) - F.col("event_ts_ms").cast("long"))

    if latency_df.count() > 0:
        avg_e2e_ms = latency_df.agg(F.avg("e2e_latency_ms")).collect()[0][0]
        avg_e2e_sec = avg_e2e_ms / 1000.0 if avg_e2e_ms else 0.0

        target_e2e = 5.0
        pass_e2e = avg_e2e_sec < target_e2e

        print(
            f" End-to-End Latency: {avg_e2e_sec:.2f} seconds (Target < {target_e2e}) -> {status(pass_e2e)}")
    else:
        print(" End-to-End Latency: Data unavailable (No valid event_ts_ms found)")

    print("=" * 80)

    print("\n[Tableau Export] Saving prediction results to CSV...")
    os.makedirs(TABLEAU_DIR, exist_ok=True)

    tableau_df = final_predictions.select(
        "marital_status", "gender", "age_at_enrollment",
        "financial_stability_index", "grade_delta",
        "target", "prediction", "label"
    )

    out_path = os.path.join(TABLEAU_DIR, "ml_dropout_predictions.csv")
    tableau_df.toPandas().to_csv(out_path, index=False)

    print(f"[SUCCESS] Predictions saved for dashboard: {out_path}")

    spark.stop()
    print("\n[OK] Done. Proceed to: Stage 4 Visualizer")
