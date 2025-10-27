#!/usr/bin/env python3
# problem2.py — Cluster Usage Analysis
# Outputs:
#   data/output/problem2_timeline.csv
#   data/output/problem2_cluster_summary.csv
#   data/output/problem2_stats.txt
#   data/output/problem2_bar_chart.png
#   data/output/problem2_density_plot.png

from __future__ import annotations

import os
import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,expr,
    regexp_extract,
    input_file_name,
    lpad,
    min as spark_min,
    max as spark_max,
    length,
)

# Optional imports: try_to_timestamp may not exist on older Spark;
# we’ll detect at runtime and fallback to expr(...) if needed.
try:
    from pyspark.sql.functions import try_to_timestamp  # Spark 3.3+
    HAVE_TRY_TO_TIMESTAMP = True
except Exception:
    from pyspark.sql.functions import expr
    HAVE_TRY_TO_TIMESTAMP = False


# --------------------------
# Spark / IO helpers
# --------------------------
def build_spark(master_url: str | None) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("Problem2-ClusterUsageAnalysis")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.ansi.enabled", "false")  # be tolerant
    )
    if master_url:
        builder = builder.master(master_url)
    return builder.getOrCreate()


def resolve_input_path(cli_input: str | None) -> str:
    """
    Priority:
      1) --input
      2) $SPARK_LOGS_BUCKET/data
      3) data/sample (if exists) else data/raw
    Ensure s3a:// scheme for Spark.
    """
    env_bucket = os.environ.get("SPARK_LOGS_BUCKET")

    if cli_input:
        path = cli_input.rstrip("/")
    elif env_bucket:
        path = f"{env_bucket.rstrip('/')}/data"
    elif Path("data/sample").exists():
        path = "data/sample"
    else:
        path = "data/raw"

    if path.startswith("s3://"):
        path = "s3a://" + path[len("s3://"):]
    return path


def ensure_outdir(outdir: str) -> Path:
    p = Path(outdir)
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_stats_text(path: Path, num_clusters: int, num_apps: int, per_cluster_counts: pd.Series):
    with path.open("w") as f:
        f.write(f"Total unique clusters: {num_clusters}\n")
        f.write(f"Total applications: {num_apps}\n")
        avg = (num_apps / num_clusters) if num_clusters else 0.0
        f.write(f"Average applications per cluster: {avg:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for cid, cnt in per_cluster_counts.sort_values(ascending=False).items():
            f.write(f"  Cluster {cid}: {cnt} applications\n")


# --------------------------
# Plotting
# --------------------------
def make_bar_chart(df_summary: pd.DataFrame, out_png: Path):
    plt.figure(figsize=(10, 5))
    ax = sns.barplot(
        data=df_summary.sort_values("num_applications", ascending=False),
        x="cluster_id", y="num_applications", palette="tab10"
    )
    ax.set_title("Applications per Cluster")
    ax.set_xlabel("Cluster ID")
    ax.set_ylabel("Number of applications")
    # value labels
    for p in ax.patches:
        ax.annotate(
            f"{int(p.get_height())}",
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center", va="bottom", fontsize=9,
            xytext=(0, 3), textcoords="offset points",
        )
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()


def make_duration_density_for_largest_cluster(df_timeline: pd.DataFrame, out_png: Path):
    # Pick cluster with most applications
    top_cluster = df_timeline["cluster_id"].value_counts().sort_values(ascending=False).index[0]
    sub = df_timeline[df_timeline["cluster_id"] == top_cluster].copy()
    plt.figure(figsize=(10, 5))
    ax = sns.histplot(sub["duration_seconds"], kde=True, bins=50)
    ax.set_xscale("log")
    ax.set_xlabel("Job duration (seconds, log-scale)")
    ax.set_ylabel("Count")
    ax.set_title(f"Duration distribution (cluster {top_cluster}, n={len(sub)})")
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()


# --------------------------
# Main
# --------------------------
def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument(
        "master", nargs="?", default=None,
        help="Spark master URL (e.g., spark://10.0.0.5:7077). Leave empty for local mode."
    )
    parser.add_argument("--net-id", required=False, help="Your NET ID (for logging only).")
    parser.add_argument("--input", default=None, help="Input root (e.g., s3a://bucket/data or local dir).")
    parser.add_argument("--outdir", default="data/output", help="Output directory.")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark and regenerate charts from existing CSVs.")
    args = parser.parse_args()

    outdir = ensure_outdir(args.outdir)

    timeline_csv = outdir / "problem2_timeline.csv"
    summary_csv  = outdir / "problem2_cluster_summary.csv"
    stats_txt    = outdir / "problem2_stats.txt"
    bar_png      = outdir / "problem2_bar_chart.png"
    dens_png     = outdir / "problem2_density_plot.png"

    if not args.skip_spark:
        input_path = resolve_input_path(args.input)
        spark = build_spark(args.master)
        sc = spark.sparkContext
        print(f"[INFO] Using master: {sc.master}")
        print(f"[INFO] Reading from: {input_path}")
        print(f"[INFO] Writing outputs to: {outdir.resolve()}")

        # Read all logs (recursive)
        logs = (
            spark.read.option("recursiveFileLookup", "true")
            .text(input_path)
            .withColumn("file_path", input_file_name())
        )

        # IDs from file path
        logs = logs.withColumn(
            "application_id",
            regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1)
        ).withColumn(
            "cluster_id",
            regexp_extract(col("application_id"), r"application_(\d+)_\d+", 1)
        ).withColumn(
            "app_number_raw",
            regexp_extract(col("application_id"), r"application_\d+_(\d+)", 1)
        ).withColumn("app_number", lpad(col("app_number_raw"), 4, "0"))

        # Parse timestamps tolerantly
        # --- extract timestamp string from the log line
        logs = logs.withColumn(
            "ts_str",
            regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
        )

        # --- robust timestamp parse (SQL expression ensures the fmt is a literal)
        logs = logs.withColumn(
            "ts",
            expr("try_to_timestamp(ts_str, 'yy/MM/dd HH:mm:ss')")
        )

        # --- filter to good rows only
        parsed = logs.where(
            (col("application_id") != "") &
            (col("cluster_id") != "") &
            (length(col("ts_str")) > 0) &
            col("ts").isNotNull()
        ).select("cluster_id", "application_id", "app_number", "ts")

        # Per application start/end
        per_app = (
            parsed.groupBy("cluster_id", "application_id", "app_number")
            .agg(
                spark_min("ts").alias("start_time"),
                spark_max("ts").alias("end_time")
            )
        )

        # Collect small results to pandas (OK for 194 apps)
        timeline_pdf = per_app.toPandas()
        timeline_pdf["start_time"] = pd.to_datetime(timeline_pdf["start_time"])
        timeline_pdf["end_time"]   = pd.to_datetime(timeline_pdf["end_time"])
        timeline_pdf["duration_seconds"] = (
            (timeline_pdf["end_time"] - timeline_pdf["start_time"]).dt.total_seconds()
        ).clip(lower=0)

        # Save timeline
        timeline_pdf = timeline_pdf.sort_values(["cluster_id", "start_time", "application_id"])
        timeline_pdf[["cluster_id", "application_id", "app_number", "start_time", "end_time", "duration_seconds"]].to_csv(
            timeline_csv, index=False
        )

        # Cluster summary
        grp = timeline_pdf.groupby("cluster_id", as_index=False).agg(
            num_applications=("application_id", "count"),
            cluster_first_app=("start_time", "min"),
            cluster_last_app=("end_time", "max"),
        ).sort_values("num_applications", ascending=False)
        grp.to_csv(summary_csv, index=False)

        # Stats
        write_stats_text(
            stats_txt,
            num_clusters=grp["cluster_id"].nunique(),
            num_apps=int(grp["num_applications"].sum()),
            per_cluster_counts=grp.set_index("cluster_id")["num_applications"],
        )

        # Charts
        sns.set_theme(style="whitegrid")
        make_bar_chart(grp, bar_png)
        if len(timeline_pdf) > 0:
            make_duration_density_for_largest_cluster(timeline_pdf, dens_png)

        spark.stop()

    else:
        # Fast path: rebuild charts/stats from existing CSVs
        timeline_pdf = pd.read_csv(timeline_csv, parse_dates=["start_time", "end_time"])
        grp = pd.read_csv(summary_csv, parse_dates=["cluster_first_app", "cluster_last_app"])

        write_stats_text(
            stats_txt,
            num_clusters=grp["cluster_id"].nunique(),
            num_apps=int(grp["num_applications"].sum()),
            per_cluster_counts=grp.set_index("cluster_id")["num_applications"],
        )
        sns.set_theme(style="whitegrid")
        make_bar_chart(grp, bar_png)
        if len(timeline_pdf) > 0:
            make_duration_density_for_largest_cluster(timeline_pdf, dens_png)

    print("[SUCCESS] Wrote:")
    for p in [timeline_csv, summary_csv, stats_txt, bar_png, dens_png]:
        print(f"  {p}")


if __name__ == "__main__":
    main()
