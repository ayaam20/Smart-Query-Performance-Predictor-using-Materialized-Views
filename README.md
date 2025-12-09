# Smart Query Performance Predictor using Materialised Views

## Overview

This project speeds up **repeated analytical queries** in PostgreSQL by
**predicting the next query** and **precomputing its result** using materialised
views.

Dashboards often run the same queries many times (e.g., “sales by category
today”, “revenue by country today”) on a large `orders` table. Each query
scans and aggregates a lot of data, so users repeatedly wait for similar slow
results. This system learns simple query patterns from a time‑stamped log and,
when there is enough idle time, refreshes the materialised view for the next
likely query in advance.

**Key idea:**  
Refresh a view only if the next query is likely *and* the view’s build time is
smaller than the time gap until that query arrives.

---

## Components

- `load_real_dataset.py`  
  Creates the `ecommerce_predictor` PostgreSQL database, defines the tables
  (`countries`, `products`, `customers`, `orders`), and loads a real e‑commerce
  CSV.

- `query_templates.py`  
  Defines five analytical queries (Q1–Q5), such as:
  - Q1: sales by category today  
  - Q2: customers by country today  
  - Q3: quantity by category today  
  - Q4: revenue by country today  
  - Q5: top‑10 products today  

- `materialised_views.py`  
  Creates one materialised view per query (V1–V5), precomputing each query’s
  result for `CURRENT_DATE`.

- `query.py`  
  Generates a 7‑day time‑stamped query log (`query_log.csv`) with realistic
  patterns like Q1→Q2→Q3 and Q1→Q4→Q3 plus occasional Q5.

- `frequency_predict.py`  
  Builds a simple frequency‑based next‑query predictor from `query_log.csv`:
  - `predict_next(current_query)`  
  - `get_confidence(current_query, next_query)`

- `controller.py`  
  Implements the **Smart Controller**:
  - measures refresh time for each materialised view  
  - during log replay, predicts the next query  
  - compares view build time vs inter‑arrival time  
  - decides whether to refresh the predicted view

- `performance.py`  
  Runs:
  - **baseline**: queries on base tables only  
  - **smart**: queries with prediction + pre‑materialisation  
  and logs average runtimes and speedup.

---

## Typical Usage

1. Load the e‑commerce dataset into PostgreSQL with `load_real_dataset.py`.
2. Create the five materialised views with `materialised_views.py`.
3. Generate a 7‑day query log using `query.py`.
4. Compare baseline vs smart execution using `performance.py` and inspect the
   reported average times and speedup.

