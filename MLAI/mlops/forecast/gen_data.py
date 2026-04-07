from snowflake.snowpark import Session

import numpy as np
import pandas as pd
from datetime import date

session = Session.builder.config("connection_name", "FRI_SF").create()
session.use_database("LSP_DB")
session.use_schema("RAW_DATA")
session.use_warehouse("ML_WH")

# ─────────────────────────────────────────────
# 1. Generate synthetic sales data
# ─────────────────────────────────────────────
np.random.seed(42)

products = {
    "P001": {"name": "Widget A", "base_demand": 150, "price": 29.99},
    "P002": {"name": "Widget B", "base_demand": 90,  "price": 49.99},
    "P003": {"name": "Gadget X", "base_demand": 60,  "price": 89.99},
    "P004": {"name": "Gadget Y", "base_demand": 200, "price": 19.99},
    "P005": {"name": "Device Z", "base_demand": 40,  "price": 129.99},
    "P006": {"name": "Accessory 1", "base_demand": 300, "price": 9.99},
    "P007": {"name": "Accessory 2", "base_demand": 250, "price": 14.99},
    "P008": {"name": "Premium A",  "base_demand": 30,  "price": 199.99},
    "P009": {"name": "Premium B",  "base_demand": 25,  "price": 249.99},
    "P010": {"name": "Bundle XY",  "base_demand": 75,  "price": 69.99},
}

stores = ["STORE_001", "STORE_002", "STORE_003", "STORE_004", "STORE_005"]
categories = {
    "P001": "Widgets", "P002": "Widgets", "P003": "Gadgets",
    "P004": "Gadgets", "P005": "Devices", "P006": "Accessories",
    "P007": "Accessories", "P008": "Premium", "P009": "Premium",
    "P010": "Bundles",
}

start_date = date(2025, 1, 1)
end_date   = date(2026, 4, 7)
date_range = pd.date_range(start=start_date, end=end_date, freq="D")

rows = []
for dt in date_range:
    dow        = dt.dayofweek          # 0=Mon … 6=Sun
    month      = dt.month
    is_weekend = int(dow >= 5)

    # Seasonal multiplier (Q4 holiday boost)
    season_mult = 1.0
    if month in [11, 12]:
        season_mult = 1.4
    elif month in [6, 7, 8]:
        season_mult = 1.15
    elif month in [1, 2]:
        season_mult = 0.85

    # Weekend multiplier
    weekend_mult = 1.3 if is_weekend else 1.0

    # Black Friday / Cyber Monday boost
    is_promo = int(
        (month == 11 and dt.day in range(25, 30)) or
        (month == 12 and dt.day in [26, 27])
    )
    promo_mult = 2.0 if is_promo else 1.0

    for product_id, info in products.items():
        for store_id in stores:
            # Store-level scaling factor
            store_factor = {"STORE_001": 1.2, "STORE_002": 1.0, "STORE_003": 0.8,
                            "STORE_004": 1.1, "STORE_005": 0.9}[store_id]

            mean_demand = (
                info["base_demand"]
                * season_mult
                * weekend_mult
                * promo_mult
                * store_factor
            )
            units_sold = max(0, int(np.random.poisson(mean_demand)))

            rows.append({
                "SALE_DATE":   dt.date(),
                "PRODUCT_ID":  product_id,
                "PRODUCT_NAME": info["name"],
                "CATEGORY":    categories[product_id],
                "STORE_ID":    store_id,
                "UNITS_SOLD":  units_sold,
                "UNIT_PRICE":  info["price"],
                "IS_PROMO":    is_promo,
            })

sales_df = pd.DataFrame(rows)
print(f"Generated {len(sales_df):,} rows | date range: {start_date} → {end_date}")

# Write to snowflake
sf_df = session.create_dataframe(sales_df)
sf_df.write.mode("overwrite").save_as_table("DAILY_SALES")