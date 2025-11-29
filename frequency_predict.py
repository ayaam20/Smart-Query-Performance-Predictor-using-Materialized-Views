import pandas as pd


class FrequencyPredictor:
    """Predict next queries based on frequency analysis of query logs."""

    def __init__(self, log_file):
        """Initialize the predictor with a query log file."""
        self.log = pd.read_csv(log_file)
        self.transitions = self._build_transition_matrix()

    def _build_transition_matrix(self):
        """Build a transition matrix from the query log."""
        transitions = {}
        for i in range(len(self.log) - 1):
            curr_query = self.log.iloc[i]["query_id"]
            next_query = self.log.iloc[i + 1]["query_id"]
            if curr_query not in transitions:
                transitions[curr_query] = {}
            transitions[curr_query][next_query] = (
                transitions[curr_query].get(next_query, 0) + 1
            )
        return transitions

    def predict_next(self, current_query):
        """Predict the next query based on the most common transition."""
        if current_query not in self.transitions:
            return None
        next_queries = self.transitions[current_query]
        return max(next_queries, key=next_queries.get)

    def get_confidence(self, current_query, next_query):
        """Get confidence score for a specific query transition."""
        if current_query not in self.transitions:
            return 0

        next_queries = self.transitions[current_query]
        total = sum(next_queries.values())
        most_common_count = next_queries.get(next_query, 0)
        return most_common_count / total if total > 0 else 0


if __name__ == "__main__":
    predictor = FrequencyPredictor("query_log.csv")
    print("Learned transitions:")
    for query, nexts in predictor.transitions.items():
        print(f"  After {query}:")
        for next_q, count in nexts.items():
            print(f"    → {next_q} ({count} times)")

    print("\nPredictions:")
    for query in ["Q1", "Q2", "Q3", "Q4", "Q5"]:
        predicted = predictor.predict_next(query)
        confidence = predictor.get_confidence(query, predicted)
        if predicted:
            print(
                f"  After {query} → Predict {predicted} "
                f"({confidence * 100:.0f}% confident)"
            )
        else:
            print(f"  After {query} → Cannot predict (no history)")