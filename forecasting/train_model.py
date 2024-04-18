#
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from typing import Tuple
from hyperopt import hp, fmin, tpe, STATUS_OK, Trials

def train_test_split(df: pd.DataFrame, train_ratio: float = 0.8) -> Tuple[pd.DataFrame, pd.DataFrame]:
    split_idx = int(len(df) * train_ratio)
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]
    return train, test

def optimize_hyperparameters(train):
    space = {
        'changepoint_prior_scale': hp.uniform('changepoint_prior_scale', 0.001, 0.5),
        'seasonality_prior_scale': hp.uniform('seasonality_prior_scale', 10, 20),
    }
#
    def objective(params):
        m = Prophet(**params)
        m.fit(train)
        df_cv = cross_validation(m, initial='60 days', period='30 days', horizon='30 days')
        df_p = performance_metrics(df_cv)
        return {'loss': df_p['mape'].mean(), 'status': STATUS_OK}

    trials = Trials()
    best = fmin(
        fn=objective,
        space=space,
        algo=tpe.suggest,
        max_evals=100,
        trials=trials
    )
    return best

def create_forecast(prophet_df, best_params, test, N_DAYS_FUTURE):
    model = Prophet(**best_params)
    model.fit(prophet_df)
    future = model.make_future_dataframe(periods=len(test) + N_DAYS_FUTURE)
    return model.predict(future)
