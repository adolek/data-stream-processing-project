from river import datasets
from river import metrics
from river import optim
from river.ensemble import BaggingRegressor
from river.neighbors import KNNRegressor
from river.tree import HoeffdingTreeRegressor
from river.preprocessing import OneHotEncoder, StandardScaler
from river import compose
import time
import pandas as pd


def print_progress(sample_id, mae, mse):
    print(f'Samples processed: {sample_id}')
    print(mae)
    print(mse)

def evaluate(stream, model, n_wait=1000, verbose=False):
    dates = []
    mae = metrics.MAE()
    mae_rolling = metrics.Rolling(metric=metrics.MAE(), window_size=n_wait)
    mse = metrics.MSE()
    mse_rolling = metrics.Rolling(metric=metrics.MSE(), window_size=n_wait)
    r2 = metrics.R2()
    r2_rolling = metrics.Rolling(metric=metrics.R2(), window_size=n_wait)
    raw_results = []
    preprocesser = compose.Discard('timestamp') + compose.Discard('symbol')
    model_name = model.__class__.__name__
    training_time = 0
    testing_time = 0
    for i, (x, y) in enumerate(stream):
        # Predict
        cur_date = x['timestamp']
        preprocesser.learn_one(x)
        x = preprocesser.transform_one(x)
        t = time.time()
        y_pred = model.predict_one(x)
        testing_time += time.time() - t
        # Update metrics and results
        mae.update(y_true=y, y_pred=y_pred)
        mae_rolling.update(y_true=y, y_pred=y_pred)
        mse.update(y_true=y, y_pred=y_pred)
        mse_rolling.update(y_true=y, y_pred=y_pred)
        r2.update(y_true=y, y_pred=y_pred)
        r2_rolling.update(y_true=y, y_pred=y_pred)
        if i % n_wait == 0 and i > 0:
            if verbose:
                print_progress(i, mae, mse, r2)
            dates.append(cur_date)
            raw_results.append([model_name, i, mae.get(), mae_rolling.get(), mse.get(), mse_rolling.get(), r2.get(), r2_rolling.get()])
        # Learn (train)
        t = time.time()
        model.learn_one(x, y)
        training_time += time.time() - t
    print_progress(i, mae, mse)
    print('Training time :', training_time)
    print('Testing time :', testing_time)
    return pd.DataFrame(raw_results, columns=['model', 'id', 'mae', 'mae_roll', 'mse', 'mse_roll', 'r2', 'r2_roll']), dates