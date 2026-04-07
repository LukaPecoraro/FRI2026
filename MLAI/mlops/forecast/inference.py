from snowflake.snowpark import Session, functions as F
from snowflake.snowpark.context import get_active_session
from snowflake.ml.registry import Registry
from snowflake.ml.feature_store import FeatureStore

session = Session.builder.config("connection_name", "FRI_SF").create()
session = get_active_session()
session.use_database("LSP_DB")
session.use_warehouse("ML_WH")

FEATURES = [
    "AVG_UNITS_7D", "AVG_UNITS_14D", "AVG_UNITS_28D",
    "STD_UNITS_7D", "STD_UNITS_28D",
    "UNITS_LAG_7D", "UNITS_LAG_14D",
    "MAX_UNITS_7D", "MAX_UNITS_28D",
    "DAY_OF_WEEK", "MONTH", "QUARTER", "DAY_OF_YEAR",
    "IS_WEEKEND", "IS_PROMO", "IS_Q4", "IS_SUMMER",
]

# ── Build inference spine: every (product, store) pair for today ──
# The spine only needs entity keys + a timestamp — no labels.
inference_date = "2026-04-07"

spine_df = (
    session.table("LSP_DB.RAW_DATA.DAILY_SALES")
    .select("PRODUCT_ID", "STORE_ID")
    .distinct()
    .with_column("SALE_DATE", F.lit(inference_date).cast("date"))
)

# ── Feature Store retrieval (point-in-time join, same as training) ──
fs = FeatureStore(
    session=session,
    database="LSP_DB",
    name="FEATURE_STORE",
    default_warehouse="ML_WH",
)

ts_fv = fs.get_feature_view("TS_DEMAND_FEATURES", "v1")
cal_fv = fs.get_feature_view("CALENDAR_FEATURES", "v1")

inference_set = fs.retrieve_feature_values(
    spine_df=spine_df,
    features=[ts_fv, cal_fv],
    spine_timestamp_col="SALE_DATE",
)

prediction_df = inference_set.to_pandas()
prediction_df = prediction_df[["PRODUCT_ID", "STORE_ID"] + FEATURES]

# ── Run model ──
reg = Registry(session=session, database_name="LSP_DB", schema_name="MODELS")
mv = reg.get_model("DEMAND_FORECAST_XGBOOST").version("V1")
df_out = mv.run(prediction_df, function_name="PREDICT")
print(df_out)