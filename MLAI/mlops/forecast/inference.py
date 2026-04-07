from snowflake.snowpark import Session, functions as F, dataframe as spdf
from snowflake.snowpark.context import get_active_session
from snowflake.ml.registry import Registry
from snowflake.ml.feature_store import FeatureStore
import pandas as pd

FEATURES = [
    "AVG_UNITS_7D", "AVG_UNITS_14D", "AVG_UNITS_28D",
    "STD_UNITS_7D", "STD_UNITS_28D",
    "UNITS_LAG_7D", "UNITS_LAG_14D",
    "MAX_UNITS_7D", "MAX_UNITS_28D",
    "DAY_OF_WEEK", "MONTH", "QUARTER", "DAY_OF_YEAR",
    "IS_WEEKEND", "IS_PROMO", "IS_Q4", "IS_SUMMER",
]

def predict(session: Session, inference_date: str) -> str:
    """Generate demand forecasts for every (product, store) on the given date."""

    # ── Build inference spine ──
    spine_df = (
        session.table("LSP_DB.RAW_DATA.DAILY_SALES")
        .select("PRODUCT_ID", "STORE_ID")
        .distinct()
        .with_column("SALE_DATE", F.lit(inference_date).cast("date"))
    )

    # ── Feature Store retrieval ──
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
    results = mv.run(prediction_df, function_name="PREDICT")
    
    # output_feature_0 is output column, rename to PREDICTION
    results = results.rename(columns={"output_feature_0": "PREDICTION"})

    # ── Write predictions to table ──
    results["FORECAST_DATE"] = inference_date
    results["PRODUCT_ID"] = prediction_df["PRODUCT_ID"]
    results["STORE_ID"] = prediction_df["STORE_ID"]
    results = results[["FORECAST_DATE", "PRODUCT_ID", "STORE_ID", "PREDICTION"]]

    session.write_pandas(results, "DEMAND_PREDICTIONS", schema="MODELS", database="LSP_DB", auto_create_table=True)
    return f"Wrote {len(results)} predictions for {inference_date}"


session = Session.builder.config("connection_name", "FRI_SF").create()
session.use_database("LSP_DB")
session.use_warehouse("ML_WH")

result_predict = predict(session, "2026-04-07")
print(result_predict)

session.sproc.register(
    func=predict,
    name="LSP_DB.MODELS.DAILY_DEMAND_FORECAST",
    is_permanent=True,
    stage_location="@LSP_DB.MODELS.SPROC_STAGE",
    replace=True,
    packages=[
        "snowflake-snowpark-python",
        "snowflake-ml-python",
    ],
)

# test SP
result_sproc = session.call("LSP_DB.MODELS.DAILY_DEMAND_FORECAST", "2026-04-07")
print(result_sproc)