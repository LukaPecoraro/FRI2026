from snowflake.snowpark import Session, functions as F
from snowflake.snowpark.window import Window
from snowflake.ml.feature_store import (
    FeatureStore,
    FeatureView,
    Entity,
    CreationMode,
)

session = Session.builder.config("connection_name", "FRI_SF").create()
session.use_database("LSP_DB")
session.use_schema("FEATURES")
session.use_warehouse("ML_WH")

# ─────────────────────────────────────────────
# 1. Initialise Feature Store
#    All Feature Store objects live in the
#    LSP_DB.FEATURE_STORE schema.
# ─────────────────────────────────────────────
fs = FeatureStore(
    session=session,
    database="LSP_DB",
    name="FEATURE_STORE",           # maps to schema name
    default_warehouse="ML_WH",
    creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
)
print("✅  Feature Store connected:", fs)

# ─────────────────────────────────────────────
# 2. Entity: product × store
#    join_keys are the columns used to join
#    feature views to the spine (label table).
# ─────────────────────────────────────────────
product_store_entity = Entity(
    name="PRODUCT_STORE",
    join_keys=["PRODUCT_ID", "STORE_ID"],
    desc="Composite entity representing a (product, store) pair",
)
fs.register_entity(product_store_entity)
print("✅  Entity registered:", product_store_entity.name)

# ─────────────────────────────────────────────
# 3. Feature View 1: Rolling time-series features
#    Computed from RAW_DATA.DAILY_SALES using
#    Snowpark window functions.
# ─────────────────────────────────────────────
raw_sales = session.table("LSP_DB.RAW_DATA.DAILY_SALES")

w7  = Window.partition_by("PRODUCT_ID", "STORE_ID").order_by("SALE_DATE").rows_between(-6, 0)
w14 = Window.partition_by("PRODUCT_ID", "STORE_ID").order_by("SALE_DATE").rows_between(-13, 0)
w28 = Window.partition_by("PRODUCT_ID", "STORE_ID").order_by("SALE_DATE").rows_between(-27, 0)

ts_feature_df = raw_sales.select(
    "PRODUCT_ID",
    "STORE_ID",
    "SALE_DATE",

    # Rolling averages
    F.avg("UNITS_SOLD").over(w7).alias("AVG_UNITS_7D"),
    F.avg("UNITS_SOLD").over(w14).alias("AVG_UNITS_14D"),
    F.avg("UNITS_SOLD").over(w28).alias("AVG_UNITS_28D"),

    # Rolling std-dev (demand volatility)
    F.stddev("UNITS_SOLD").over(w7).alias("STD_UNITS_7D"),
    F.stddev("UNITS_SOLD").over(w28).alias("STD_UNITS_28D"),

    # Lag features (same day last week / last 2 weeks)
    F.lag("UNITS_SOLD", 7).over(
        Window.partition_by("PRODUCT_ID", "STORE_ID").order_by("SALE_DATE")
    ).alias("UNITS_LAG_7D"),
    F.lag("UNITS_SOLD", 14).over(
        Window.partition_by("PRODUCT_ID", "STORE_ID").order_by("SALE_DATE")
    ).alias("UNITS_LAG_14D"),

    # Rolling max (captures promo spikes)
    F.max("UNITS_SOLD").over(w7).alias("MAX_UNITS_7D"),
    F.max("UNITS_SOLD").over(w28).alias("MAX_UNITS_28D"),
)

ts_fv = FeatureView(
    name="TS_DEMAND_FEATURES",
    entities=[product_store_entity],
    feature_df=ts_feature_df,
    timestamp_col="SALE_DATE",
    refresh_freq="1 day",           # incremental refresh daily
    desc="Rolling time-series demand features (7/14/28-day windows)",
    warehouse="ML_WH",
)
ts_fv = ts_fv.attach_feature_desc({
    "AVG_UNITS_7D":    "7-day rolling average units sold",
    "AVG_UNITS_14D":   "14-day rolling average units sold",
    "AVG_UNITS_28D":   "28-day rolling average units sold",
    "STD_UNITS_7D":    "7-day rolling std-dev of units sold",
    "STD_UNITS_28D":   "28-day rolling std-dev of units sold",
    "UNITS_LAG_7D":    "Units sold 7 days ago",
    "UNITS_LAG_14D":   "Units sold 14 days ago",
    "MAX_UNITS_7D":    "Max units sold in last 7 days",
    "MAX_UNITS_28D":   "Max units sold in last 28 days",
})
registered_ts_fv = fs.register_feature_view(
    ts_fv, version="v1", block=True, overwrite=True
)
print("✅  FeatureView registered:", registered_ts_fv.name, "status:", registered_ts_fv.status)

# ─────────────────────────────────────────────
# 4. Feature View 2: Calendar / product metadata features
#    These are static/slow-moving features.
# ─────────────────────────────────────────────
calendar_feature_df = raw_sales.select(
    "PRODUCT_ID",
    "STORE_ID",
    "SALE_DATE",

    # Calendar
    F.dayofweek("SALE_DATE").alias("DAY_OF_WEEK"),
    F.month("SALE_DATE").alias("MONTH"),
    F.quarter("SALE_DATE").alias("QUARTER"),
    F.dayofyear("SALE_DATE").alias("DAY_OF_YEAR"),

    (F.dayofweek("SALE_DATE").isin([5, 6])).cast("int").alias("IS_WEEKEND"),

    "IS_PROMO",

    # Derived: is Q4 (holiday season)
    (F.month("SALE_DATE").isin([10, 11, 12])).cast("int").alias("IS_Q4"),
    # Derived: is summer
    (F.month("SALE_DATE").isin([6, 7, 8])).cast("int").alias("IS_SUMMER"),
)

cal_fv = FeatureView(
    name="CALENDAR_FEATURES",
    entities=[product_store_entity],
    feature_df=calendar_feature_df,
    timestamp_col="SALE_DATE",
    refresh_freq="1 day",
    desc="Calendar and product metadata features for demand forecasting",
    warehouse="ML_WH",
)
cal_fv = cal_fv.attach_feature_desc({
    "DAY_OF_WEEK": "Day of week (0=Mon, 6=Sun)",
    "MONTH":       "Month number (1-12)",
    "QUARTER":     "Quarter number (1-4)",
    "DAY_OF_YEAR": "Day of year (1-366)",
    "IS_WEEKEND":  "1 if Saturday or Sunday",
    "IS_PROMO":    "1 if promotional period",
    "IS_Q4":       "1 if Q4 (holiday season)",
    "IS_SUMMER":   "1 if summer months (Jun-Aug)",
})
registered_cal_fv = fs.register_feature_view(
    cal_fv, version="v1", block=True, overwrite=True
)
print("✅  FeatureView registered:", registered_cal_fv.name, "status:", registered_cal_fv.status)

# ─────────────────────────────────────────────
# 5. List all Feature Views
# ─────────────────────────────────────────────
print("\n── Registered Feature Views ──")
fs.list_feature_views().select('name', 'version', 'desc', 'refresh_freq').show()

# ─────────────────────────────────────────────
# 6. Generate a training dataset
#    Spine = all (PRODUCT_ID, STORE_ID, SALE_DATE, UNITS_SOLD) rows
#    Feature Store point-in-time joins the FeatureViews onto the spine.
# ─────────────────────────────────────────────
spine_df = session.table("LSP_DB.RAW_DATA.DAILY_SALES").select(
    "PRODUCT_ID",
    "STORE_ID",
    "SALE_DATE",
    "UNITS_SOLD",   # label / target
)

# Materialize
training_set = fs.generate_training_set(
    spine_df=spine_df,
    features=[registered_ts_fv, registered_cal_fv],
    spine_timestamp_col="SALE_DATE",
    save_as="LSP_DB.FEATURE_STORE.TRAINING_SET",  # materialise as table
)
