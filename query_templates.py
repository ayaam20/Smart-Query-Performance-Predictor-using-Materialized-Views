queries = {
    'Q1': """
        SELECT p.category, SUM(o.quantity * o.unit_price) AS total_sales
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        WHERE o.order_date = CURRENT_DATE
        GROUP BY p.category
        ORDER BY total_sales DESC;
    """,
    
    'Q2': """
        SELECT c.country_name, COUNT(DISTINCT o.customer_id) AS unique_customers
        FROM orders o
        JOIN countries c ON o.country_id = c.country_id
        WHERE o.order_date = CURRENT_DATE
        GROUP BY c.country_name
        ORDER BY unique_customers DESC;
    """,
    
    'Q3': """
        SELECT p.category, SUM(o.quantity) AS total_quantity
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        WHERE o.order_date = CURRENT_DATE
        GROUP BY p.category
        ORDER BY total_quantity DESC;
    """,
    
    'Q4': """
        SELECT c.country_name, SUM(o.quantity * o.unit_price) AS revenue
        FROM orders o
        JOIN countries c ON o.country_id = c.country_id
        WHERE o.order_date = CURRENT_DATE
        GROUP BY c.country_name
        ORDER BY revenue DESC;
    """,
    
    'Q5': """
        SELECT p.description, SUM(o.quantity) AS total_sold
        FROM orders o
        JOIN products p ON o.product_id = p.product_id
        WHERE o.order_date = CURRENT_DATE
        GROUP BY p.description
        ORDER BY total_sold DESC
        LIMIT 10;
    """
}

query_descriptions = {
    'Q1': 'Sales by Product Category Today',
    'Q2': 'Unique Customers by Country Today',
    'Q3': 'Total Quantity by Category Today',
    'Q4': 'Revenue by Country Today',
    'Q5': 'Top 10 Products Sold Today'
}