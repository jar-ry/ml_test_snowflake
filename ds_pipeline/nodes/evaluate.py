import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, r2_score
import pickle
import shap
import matplotlib.pyplot as pl

from helper.data_helper import get_data_path, get_data_path, save_dataframes

def evaluate(input_data: list[str], output_data: list[str], is_local) -> pd.DataFrame:
    input_data_assets = get_data_path(input_data, is_local)
    output_data_assets = get_data_path(output_data, is_local=is_local)
    
    x_test = pd.read_csv(input_data_assets['x_test'])
    y_test = pd.read_csv(input_data_assets['y_test'])

    # load
    with open(input_data_assets['lr_model'], 'rb') as f:
        reg = pickle.load(f)

    y_pred = reg.predict(x_test)
    
    rmse = np.sqrt(mean_squared_error(y_test,y_pred))
    r2 = r2_score(y_test, y_pred)

    metrics_df = pd.DataFrame(
        {
            "metrics":[
                "rmse",
                "r2"
            ],
            "score":[rmse, r2]
        }
    )
    
    output_dict = {
        "metrics": metrics_df
    }
    
    save_dataframes(
        dataframes=output_dict,
        data_assets=output_data,
        is_local=is_local,
        save_type="csv"
    )