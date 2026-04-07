from snowflake.snowpark import Session, functions as F
from snowflake.snowpark.context import get_active_session
from snowflake.ml.registry import registry
from snowflake.ml.feature_store import (
    FeatureStore,
    FeatureView,
    Entity,
    CreationMode,
)


session = Session.builder.config("connection_name", "FRI_SF").create()
session = get_active_session()
session.use_database("LSP_DB")
session.use_warehouse("ML_WH")

fs = FeatureStore(
    session=session,
    database="LSP_DB",
    name="FEATURE_STORE",           # maps to schema name
    default_warehouse="ML_WH",
)

# Where SALE_DATE is max_date
spine_df = session.table("LSP_DB.RAW_DATA.DAILY_SALES").select(
    "PRODUCT_ID",
    "STORE_ID",
    "SALE_DATE",
    "UNITS_SOLD",   # label / target
).filter(
    F.col("SALE_DATE") == F.current_date() - F.expr("INTERVAL '1 DAY'")
)

fv_ts = fs.get_feature_view(name="TS_DEMAND_FEATURES", version="v1")
fv_cal = fs.get_feature_view(name="CALENDAR_FEATURES", version="v1")

prediction_df = fs.retrieve_feature_values(
    spine_df=spine_df,
    features=[fv_cal, fv_ts],
).to_pandas()

reg = registry.Registry(session=session, database_name='LSP_DB', schema_name='MODELS')
mv = reg.get_model('DEMAND_FORECAST_XGBOOST').version('V1')
df_out = mv.run(prediction_df, function_name='PREDICT')