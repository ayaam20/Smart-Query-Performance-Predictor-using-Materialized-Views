import psycopg2
import time
import pandas as pd
from query_templates import queries
from controller import SmartController

DB_CONFIG = {
    "dbname": "ecommerce_predictor",
    "user": "postgres",
    "password": "root",
}
BASELINE_RUNS = 3
OUTPUT_DIR = "."

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        raise


def measure_baseline():
    print("Measuring baseline query performance...")
    conn = get_db_connection()
    cur = conn.cursor()
    results = []

    for query_id, query_text in queries.items():
        times = []
        for _ in range(BASELINE_RUNS):
            try:
                start = time.time()
                cur.execute(query_text)
                elapsed = time.time() - start
                times.append(elapsed)
            except psycopg2.Error as e:
                print(f"Error executing query {query_id}: {e}")
                continue

        if times:
            avg_time = sum(times) / len(times)
            results.append({
                "query_id": query_id,
                "baseline_time_sec": avg_time,
            })

    cur.close()
    conn.close()

    baseline_df = pd.DataFrame(results)
    output_file = f"{OUTPUT_DIR}/baselines.csv"
    baseline_df.to_csv(output_file, index=False)
    print(f"Baseline measurements saved to: {output_file}\n")
    return baseline_df 


def measure_smart_performance():
    print("Measuring smart query performance with materialized views")
    log_df = pd.read_csv("query_log.csv")
    conn = get_db_connection()
    cur = conn.cursor()
    controller = SmartController('query_log.csv')
    controller.measure_mv_times()
    results = []

    for i in range(len(log_df) - 1):
        current_query = log_df.iloc[i]["query_id"]
        next_query = log_df.iloc[i + 1]["query_id"]
        current_time = pd.to_datetime(log_df.iloc[i]["timestamp"])
        next_time = pd.to_datetime(log_df.iloc[i + 1]["timestamp"])
        inter_arrival = (next_time - current_time).total_seconds()

        should_build, details = controller.should_prematerialize(
            current_query, inter_arrival
        )
        if should_build:
            success, build_time = controller.prematerialize_view(
                details["mv_name"]
            )
            if success:
                print(
                    f"  Pre-materialized {details['mv_name']} ({build_time:.4f}s) "
                    f"- Next: {details['next_query']}"
                )

        try:
            start = time.time()
            cur.execute(queries[next_query])
            runtime = time.time() - start
        except psycopg2.Error as e:
            print(f"Error executing query {next_query}: {e}")
            runtime = None

        if runtime is not None:
            results.append({
                "query_id": current_query,
                "runtime_sec": runtime,
                "prematerialized": should_build,
                "predicted_next": (
                    details["next_query"] if should_build else None
                ),
            })

        if (i + 1) % 5 == 0:
            print(f"  Processed {i + 1}/{len(log_df) - 1} queries")

    cur.close()
    conn.close()
    controller.close()

    performance_df = pd.DataFrame(results)
    output_file = f"{OUTPUT_DIR}/smart_performance.csv"
    performance_df.to_csv(output_file, index=False)
    print(f"Smart performance measurements saved to: {output_file}\n")
    return performance_df

if __name__ == "__main__":
    print("=" * 60)
    print("Smart Query Performance Predictor - Performance Analysis")
    print("=" * 60 + "\n")

    try:
        baseline_df = measure_baseline()
        smart_df = measure_smart_performance()
        baseline_avg = baseline_df["baseline_time_sec"].mean()
        smart_avg = smart_df["runtime_sec"].mean()
        speedup = baseline_avg / smart_avg
        print("=" * 60)
        print("Performance Summary")
        print("=" * 60)
        print(f"Baseline average time: {baseline_avg:.4f}s")
        print(f"Smart average time: {smart_avg:.4f}s")
        print(f"Overall speedup: {speedup:.2f}x")
        print("=" * 60)
    except Exception as e:
        print(f"Error during performance analysis: {e}")
        raise
