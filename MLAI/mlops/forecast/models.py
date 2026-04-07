import json
import pandas as pd
import xgboost as xgb
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.ml.experiment import ExperimentTracking
from snowflake.ml.registry import Registry
from sklearn.model_selection import cross_val_score
from tabicl import TabICLRegressor

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
TARGET = "UNITS_SOLD"

# ── Load data ──
df = session.table("LSP_DB.FEATURE_STORE.TRAINING_SET").to_pandas()
X, y = df[FEATURES], df[TARGET]

# ── Experiment tracking ──
exp = ExperimentTracking(session=session)
exp.set_experiment("DEMAND_FORECAST_EXPERIMENT")

# ── XGBoost: 10-fold CV ──
params = dict(
    objective="reg:tweedie", # absoluteerror, squarederror, reg:tweedie
    n_estimators=100,
    max_depth=4,
    learning_rate=0.1,
    random_state=42,
    n_jobs=-1,
)
xgb_model = xgb.XGBRegressor(**params)

cv_scores = -cross_val_score(
    xgb_model, X, y, cv=10, scoring="neg_mean_absolute_error", n_jobs=-1
)

date_time_now = pd.Timestamp.now().strftime("%Y_%m_%d_%H_%M_%S")

with exp.start_run(f"xgboost_10fold_{date_time_now}"):
    exp.log_params({"model_params": json.dumps(params)})
    exp.log_metrics({
        "cv_mae_mean": round(cv_scores.mean(), 4),
        "cv_mae_std":  round(cv_scores.std(), 4),
    })
    print(f"XGBOOST CV MAE: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

# ── Register model (fit on full dataset first) ──
xgb_model.fit(X, y)

registry = Registry(session, database_name="LSP_DB", schema_name="MODELS")
registry.log_model(
    model=xgb_model,
    model_name="DEMAND_FORECAST_XGBOOST",
    version_name="v1",
    comment="XGBoost — 10-fold CV MAE: {:.4f} ± {:.4f}".format(
        cv_scores.mean(), cv_scores.std()
    ),
    conda_dependencies=["xgboost", "scikit-learn"],
    sample_input_data=X.iloc[:100],
)

# Tabular foundation model
params = dict(
    n_estimators=1,
    outlier_threshold=4,
)
tabicl_model = TabICLRegressor(**params, verbose=True)

cv_scores = -cross_val_score(
    tabicl_model, X, y, cv=10, scoring="neg_mean_absolute_error", verbose=1)

date_time_now = pd.Timestamp.now().strftime("%Y_%m_%d_%H_%M_%S")

with exp.start_run(f"tabicl_10fold_{date_time_now}"):
    exp.log_params({"model_params": json.dumps(params)})
    exp.log_metrics({
        "cv_mae_mean": round(cv_scores.mean(), 4),
        "cv_mae_std":  round(cv_scores.std(), 4),
    })
    print(f"TABICL CV MAE: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

# ── Register model (fit on full dataset first) ──
tabicl_model.fit(X, y)
registry = Registry(session, database_name="LSP_DB", schema_name="MODELS")
registry.log_model(
    model=tabicl_model,
    model_name="TABICL_FORECASTER",
    version_name="v1",
    comment="TabICL — 10-fold CV MAE: {:.4f} ± {:.4f}".format(
        cv_scores.mean(), cv_scores.std()
    ),
    pip_requirements=["tabicl"],
    sample_input_data=X.iloc[:100],
)