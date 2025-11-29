import psycopg2
import time
from frequency_predict import FrequencyPredictor


class SmartController:
    """Controller for smart materialized view pre-materialization."""

    def __init__(
        self,
        log_file,
        db_name="ecommerce_predictor",
        db_user="postgres",
        db_password="root",
    ):
        """Initialize the smart controller with query predictor and database connection."""
        self.predictor = FrequencyPredictor(log_file)

        self.query_to_mv = {
            "Q1": "mv_sales_by_category_today",
            "Q2": "mv_customers_by_country_today",
            "Q3": "mv_quantity_by_category_today",
            "Q4": "mv_revenue_by_country_today",
            "Q5": "mv_top_products_today",
        }

        self.mv_build_times = {}

        self.conn = psycopg2.connect(
            f"dbname={db_name} user={db_user} password={db_password}"
        )
        self.cur = self.conn.cursor()

    def measure_mv_times(self):
        """Measure REFRESH time of each MV and store in mv_build_times."""
        print("Measuring MV build times...")
        for mv_name in self.query_to_mv.values():
            start = time.time()
            self.cur.execute(f"REFRESH MATERIALIZED VIEW {mv_name};")
            build_time = time.time() - start
            self.mv_build_times[mv_name] = build_time
            print(f"  {mv_name}: {build_time:.4f}s")
        self.conn.commit()

    def should_prematerialize(self, current_query, inter_arrival_time_sec):
        """Determine if a materialized view should be pre-materialized."""
        predicted_next = self.predictor.predict_next(current_query)
        if not predicted_next:
            return False, None

        mv_name = self.query_to_mv.get(predicted_next)
        if not mv_name:
            return False, None

        build_time = self.mv_build_times.get(mv_name, 10.0)
        confidence = self.predictor.get_confidence(current_query, predicted_next)
        should_build = build_time < inter_arrival_time_sec

        details = {
            "next_query": predicted_next,
            "mv_name": mv_name,
            "build_time": build_time,
            "inter_arrival_time": inter_arrival_time_sec,
            "confidence": confidence,
        }
        return should_build, details

    def prematerialize_view(self, mv_name):
        """Refresh a materialized view."""
        try:
            start = time.time()
            self.cur.execute(f"REFRESH MATERIALIZED VIEW {mv_name};")
            self.conn.commit()
            build_time = time.time() - start
            return True, build_time
        except Exception as e:
            print(f"Error refreshing {mv_name}: {e}")
            return False, 0.0

    def close(self):
        """Close database connection."""
        self.cur.close()
        self.conn.close()
