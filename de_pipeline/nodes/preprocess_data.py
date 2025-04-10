import pandas as pd
from helper.data_helper import get_data_path, save_dataframes

def preprocess_data(input_data: list[str], output_data: list[str], is_local) -> pd.DataFrame:
    data_assets = get_data_path(input_data, is_local)
    
    housing_df = pd.read_csv(data_assets['housing'])
    
    housing_df['AveRooms'] = housing_df['AveRooms'].round(2)
    housing_df['AveBedrms'] = housing_df['AveBedrms'].round(2)
    housing_df['AveOccup'] = housing_df['AveOccup'].round(2)

    output_dict = {
        "processed_housing": housing_df
    }

    save_dataframes(
        dataframes=output_dict,
        data_assets=output_data,
        is_local=is_local,
        save_type="csv"
    )