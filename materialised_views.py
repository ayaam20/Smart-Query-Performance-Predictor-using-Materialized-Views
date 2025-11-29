import psycopg2

def create_materialized_views():
    """Create materialized views for performance optimization."""
    print("Creating materialized views\n")

    conn = psycopg2.connect(
        "dbname=ecommerce_predictor user=postgres password=root"
    )
    cur = conn.cursor()

    views = {
        "mv_sales_by_category_today": """
            SELECT p.category, SUM(o.quantity * o.unit_price) AS total_sales
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE o.order_date = CURRENT_DATE
            GROUP BY p.category;
        """,
        "mv_customers_by_country_today": """
            SELECT c.country_name, COUNT(DISTINCT o.customer_id) AS unique_customers
            FROM orders o
            JOIN countries c ON o.country_id = c.country_id
            WHERE o.order_date = CURRENT_DATE
            GROUP BY c.country_name;
        """,
        "mv_quantity_by_category_today": """
            SELECT p.category, SUM(o.quantity) AS total_quantity
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE o.order_date = CURRENT_DATE
            GROUP BY p.category;
        """,
        "mv_revenue_by_country_today": """
            SELECT c.country_name, SUM(o.quantity * o.unit_price) AS revenue
            FROM orders o
            JOIN countries c ON o.country_id = c.country_id
            WHERE o.order_date = CURRENT_DATE
            GROUP BY c.country_name;
        """,
        "mv_top_products_today": """
            SELECT p.description, SUM(o.quantity) AS total_sold
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE o.order_date = CURRENT_DATE
            GROUP BY p.description
            ORDER BY total_sold DESC
            LIMIT 10;
        """
    }

    for mv_name, mv_query in views.items():
        try:
            cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name};")
        except:
            pass

        cur.execute(f"CREATE MATERIALIZED VIEW {mv_name} AS {mv_query}")
        print(f"Created materialized view: {mv_name}")

    conn.commit()
    cur.close()
    conn.close()
    print("\nMaterialized views created successfully.")

if __name__ == "__main__":
    create_materialized_views()