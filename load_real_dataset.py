import pandas as pd
import psycopg2
from datetime import datetime

def create_schema():
    conn = psycopg2.connect(
        "dbname=postgres user=postgres password=root"
    )
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("DROP DATABASE IF EXISTS ecommerce_predictor;")
        print("Dropped old database")
    except:
        pass

    cur.execute("CREATE DATABASE ecommerce_predictor;")
    print("Created new database: ecommerce_predictor")
    cur.close()
    conn.close()

    conn = psycopg2.connect(
        "dbname=ecommerce_predictor user=postgres password=root"
    )
    cur = conn.cursor()

    print("\nCreating countries table")
    cur.execute("""
        CREATE TABLE countries (
            country_id SERIAL PRIMARY KEY,
            country_name VARCHAR(100) UNIQUE
        );
    """)
    print("Created: countries")

    print("\nCreating products table")
    cur.execute("""
        CREATE TABLE products (
            product_id VARCHAR(20) PRIMARY KEY,
            description VARCHAR(255),
            category VARCHAR(100)
        );
    """)
    print("Created: products")

    print("\nCreating customers table")
    cur.execute("""
        CREATE TABLE customers (
            customer_id INT PRIMARY KEY,
            country_id INT REFERENCES countries(country_id)
        );
    """)
    print("Created: customers")

    print("\nCreating orders table")
    cur.execute("""
        CREATE TABLE orders (
            order_id SERIAL PRIMARY KEY,
            invoice_no VARCHAR(20),
            customer_id INT REFERENCES customers(customer_id),
            product_id VARCHAR(20) REFERENCES products(product_id),
            quantity INT,
            unit_price NUMERIC,
            order_date DATE,
            order_time TIME,
            country_id INT REFERENCES countries(country_id)
        );
    """)
    print("Created: orders")

    conn.commit()
    cur.close()
    conn.close()

    print("\n All  tables Created\n")

def load_data():

    df = pd.read_csv('data/data.csv', encoding='latin-1')
    df = df.dropna(subset=['CustomerID'])
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='mixed', dayfirst=True)
    df['order_date'] = df['InvoiceDate'].dt.date
    df['order_time'] = df['InvoiceDate'].dt.time
    df['category'] = df['Description'].str.split().str[0]

    print(f"   Total rows: {len(df):,}")
    print(f"   Date range: {df['order_date'].min()} to {df['order_date'].max()}")
    print(f"   Countries: {df['Country'].nunique()}")
    print(f"   Products: {df['StockCode'].nunique()}")
    print(f"   Customers: {df['CustomerID'].nunique()}")

    conn = psycopg2.connect(
        "dbname=ecommerce_predictor user=postgres password=root"
    )
    cur = conn.cursor()

    countries = df['Country'].unique()
    country_map = {}
    for i, country in enumerate(countries, 1):
        cur.execute(
            "INSERT INTO countries (country_name) VALUES (%s)",
            (country,)
        )
        country_map[country] = i
    conn.commit()

    products = df[['StockCode', 'Description']].drop_duplicates()
    for _, row in products.iterrows():
        # Get category (first word of description)
        cat = row['Description'].split()[0] if pd.notna(row['Description']) else 'Other'
        cur.execute("""
            INSERT INTO products (product_id, description, category) 
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (str(row['StockCode']), row['Description'], cat))
    conn.commit()

    customers = df[['CustomerID', 'Country']].drop_duplicates()
    for _, row in customers.iterrows():
        country_id = country_map[row['Country']]
        cur.execute("""
            INSERT INTO customers (customer_id, country_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (int(row['CustomerID']), country_id))
    conn.commit()

    for idx, row in df.iterrows():
        # Show progress every 50,000 rows
        if idx % 50000 == 0:
            print(f"   Processing row {idx:,}/{len(df):,}...")
        try:
            cur.execute("""
                INSERT INTO orders 
                (invoice_no, customer_id, product_id, quantity, unit_price, 
                 order_date, order_time, country_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                str(row['InvoiceNo']),           # invoice number
                int(row['CustomerID']),          # customer id
                str(row['StockCode']),           # product id
                int(row['Quantity']),            # quantity ordered
                float(row['UnitPrice']),         # price per unit
                row['order_date'],               # order date
                row['order_time'],               # order time
                country_map[row['Country']]      # country id
            ))
        except Exception as e:
            pass
    conn.commit()
    cur.close()
    conn.close()
    print("Database Completed")

if __name__ == "__main__":
    create_schema()
    load_data()