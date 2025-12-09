import pandas as pd
from datetime import datetime, timedelta
impminor fixeort random


def generate_query_log():
    print("Generating simulated query log data")

    query_sequence = []
    timestamps = []

    start_time = datetime(2025, 11, 24, 9, 0, 0)

    for day in range(7):
        # For Morning Session
        query_sequence.extend(["Q1", "Q2", "Q3"])
        timestamps.extend([
            start_time + timedelta(days=day, hours=1),
            start_time + timedelta(days=day, hours=1, minutes=5),
            start_time + timedelta(days=day, hours=1, minutes=10),
        ])
        # For Afternoon Session
        query_sequence.extend(["Q1", "Q4", "Q3"])
        timestamps.extend([
            start_time + timedelta(days=day, hours=6),
            start_time + timedelta(days=day, hours=6, minutes=5),
            start_time + timedelta(days=day, hours=6, minutes=10),
        ])
        # Random additional queries
        if random.random() > 0.6:
            query_sequence.append("Q5")
            timestamps.append(
                start_time + timedelta(days=day, hours=random.randint(16, 18))
            )
        # Occasional extra queries
        if random.random() > 0.7:
            query_sequence.append("Q2")
            timestamps.append(
                start_time + timedelta(days=day, hours=random.randint(14, 17))
            )

    log_df = pd.DataFrame({
        "timestamp": timestamps,
        "query_id": query_sequence,
    })
    log_df.to_csv("query_log.csv", index=False)

    print(f"\nGenerated query log with {len(log_df)} queries")
    print(f"\nQuery distribution:")
    print(log_df["query_id"].value_counts().sort_index())
    print(f"\nSample (first 10 queries):")
    print(log_df.head(10).to_string(index=False))
    print("\nSaved to: query_log.csv")

    return log_df


if __name__ == "__main__":
    generate_query_log()