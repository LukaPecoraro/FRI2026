from snowflake.snowpark import Session, Window, functions as F
from snowflake.snowpark.context import get_active_session
from snowflake.ml.registry import registry
from snowflake.ml.feature_store import (
    FeatureStore,
)

session = Session.builder.config("connection_name", "FRI_SF").create()
session = get_active_session()
session.use_database("LSP_DB")
session.use_warehouse("ML_WH")



prediction_df = session.table("LSP_DB.FEATURE_STORE.TRAINING_SET") \
    .filter(F.col("SALE_DATE") == "2026-04-07").to_pandas()

FEATURES = [
    "AVG_UNITS_7D", "AVG_UNITS_14D", "AVG_UNITS_28D",
    "STD_UNITS_7D", "STD_UNITS_28D",
    "UNITS_LAG_7D", "UNITS_LAG_14D",
    "MAX_UNITS_7D", "MAX_UNITS_28D",
    "DAY_OF_WEEK", "MONTH", "QUARTER", "DAY_OF_YEAR",
    "IS_WEEKEND", "IS_PROMO", "IS_Q4", "IS_SUMMER",
]

prediction_df = prediction_df[["PRODUCT_ID", "STORE_ID"] + FEATURES]

reg = registry.Registry(session=session, database_name='LSP_DB', schema_name='MODELS')
mv = reg.get_model('DEMAND_FORECAST_XGBOOST').version('V1')
df_out = mv.run(prediction_df, function_name='PREDICT')