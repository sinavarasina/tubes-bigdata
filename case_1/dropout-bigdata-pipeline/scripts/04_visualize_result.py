#!/usr/bin/env python3
import numpy as np
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import argparse
import glob
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")

LOG_DIR = os.environ.get("LOG_DIR", os.path.join(
    os.path.dirname(__file__), "..", "logs"))
OUT_DIR = os.path.join(LOG_DIR, "charts")
os.makedirs(OUT_DIR, exist_ok=True)

COLORS = {
    "Random Forest": "#2196F3",
    "Logistic Regression": "#4CAF50",
    "Decision Tree": "#FF9800",
    "Naive Bayes": "#9C27B0",
    "target": "#F44336",
}


def load_latest(path=None):
    if path and os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    files = sorted(glob.glob(os.path.join(LOG_DIR, "training_results_*.json")))
    if not files:
        print("[ERROR] No training result found. Run stage 3 first.")
        sys.exit(1)
    with open(files[-1]) as f:
        return json.load(f)


def plot_model_comparison(results):
    models = [r["model"] for r in results]
    metrics = ["accuracy", "precision", "recall", "f1_score"]
    labels = ["Accuracy", "Precision", "Recall", "F1-Score"]
    targets = [0.80, 0.75, 0.80, 0.77]

    x = np.arange(len(metrics))
    width = 0.18
    offset = np.linspace(-(len(models) - 1) * width / 2,
                         (len(models) - 1) * width / 2, len(models))

    fig, ax = plt.subplots(figsize=(12, 6))
    fig.patch.set_facecolor("#FAFAFA")
    ax.set_facecolor("#FAFAFA")

    for i, (model_name, off) in enumerate(zip(models, offset)):
        r = next(r for r in results if r["model"] == model_name)
        values = [r[m] for m in metrics]
        color = COLORS.get(model_name, f"C{i}")
        bars = ax.bar(x + off, values, width, label=model_name,
                      color=color, alpha=0.85, edgecolor="white", linewidth=0.5)
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2., bar.get_height() + 0.005,
                    f"{val:.3f}", ha="center", va="bottom", fontsize=7.5, fontweight="bold")

    for xt, target in zip(x, targets):
        ax.hlines(target, xt - 0.4, xt + 0.4,
                  colors=COLORS["target"], linestyles="--", linewidth=1.5, alpha=0.8)

    ax.set_xlabel("Evaluation Metric", fontsize=12, fontweight="bold")
    ax.set_ylabel("Score", fontsize=12, fontweight="bold")
    ax.set_title("Model Comparison — Case 1 (Python / PySpark)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_ylim(0, 1.08)
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    target_patch = mpatches.Patch(
        color=COLORS["target"], linestyle="--", label="Target (acuan)", alpha=0.8)
    handles, lbls = ax.get_legend_handles_labels()
    ax.legend(handles + [target_patch], lbls +
              ["Target (acuan)"], loc="lower right", fontsize=9)

    out = os.path.join(OUT_DIR, "01_model_comparison.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[OK] {out}")
    return out


def plot_feature_importance(feat_data, top_n=15):
    data = feat_data[:top_n]
    names = [d["feature"].replace("_", " ") for d in data]
    values = [d["importance"] for d in data]

    def color_for(name):
        n = name.lower()
        if any(k in n for k in ["1st", "2nd", "grade", "approved", "enrolled", "pass", "delta"]):
            return "#2196F3"
        if any(k in n for k in ["mother", "father", "scholarship", "debtor", "tuition", "financial"]):
            return "#4CAF50"
        if any(k in n for k in ["gdp", "unemployment", "inflation"]):
            return "#FF9800"
        return "#9C27B0"

    colors = [color_for(n) for n in names]
    fig, ax = plt.subplots(figsize=(10, 7))
    fig.patch.set_facecolor("#FAFAFA")
    ax.set_facecolor("#FAFAFA")

    bars = ax.barh(range(len(names)), values, color=colors, alpha=0.85,
                   edgecolor="white", linewidth=0.5)
    for bar, val in zip(bars, values):
        ax.text(val + 0.002, bar.get_y() + bar.get_height() / 2.,
                f"{val:.4f}", va="center", fontsize=8)

    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Importance Score", fontsize=11, fontweight="bold")
    ax.set_title(f"Top {top_n} Feature Importance — Random Forest (Case 1)",
                 fontsize=13, fontweight="bold", pad=15)
    ax.grid(axis="x", alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    legend = [
        mpatches.Patch(color="#2196F3", label="Academic"),
        mpatches.Patch(color="#4CAF50", label="Socio-Economic"),
        mpatches.Patch(color="#FF9800", label="Macroeconomic"),
        mpatches.Patch(color="#9C27B0", label="Demographic"),
    ]
    ax.legend(handles=legend, loc="lower right", fontsize=9)

    out = os.path.join(OUT_DIR, "02_feature_importance.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[OK] {out}")
    return out


def plot_radar(results):
    metrics = ["Accuracy", "Precision", "Recall", "F1-Score"]
    metric_keys = ["accuracy", "precision", "recall", "f1_score"]
    N = len(metrics)
    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(7, 7), subplot_kw=dict(polar=True))
    fig.patch.set_facecolor("#FAFAFA")
    ax.set_facecolor("#FAFAFA")

    for r in results:
        vals = [r[k] for k in metric_keys] + [r[metric_keys[0]]]
        color = COLORS.get(r["model"], "gray")
        ax.plot(angles, vals, "o-", linewidth=2, label=r["model"], color=color)
        ax.fill(angles, vals, alpha=0.1, color=color)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(metrics, fontsize=11)
    ax.set_ylim(0, 1)
    ax.set_title("Radar — Model Comparison (Case 1)",
                 fontsize=13, fontweight="bold", pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.1), fontsize=9)

    out = os.path.join(OUT_DIR, "03_radar_comparison.png")
    plt.tight_layout()
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[OK] {out}")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", default=None)
    args = parser.parse_args()

    print("=" * 60)
    print("CASE 1 — STAGE 4: VISUALISASI")
    print("=" * 60)

    data = load_latest(args.results)
    plot_model_comparison(data["model_comparison"])
    if data.get("feature_importance"):
        plot_feature_importance(data["feature_importance"])
    plot_radar(data["model_comparison"])

    print(f"\n[OK] All charts saved to: {OUT_DIR}")
