import pandas as pd
from helper.data_helper import get_data_path, save_dataframes

def process_data(input_data: list[str], output_data: list[str], is_local) -> pd.DataFrame:
    data_assets = get_data_path(input_data, is_local)
    
    housing_df = pd.read_csv(data_assets['processed_housing'])
    lookup_df = pd.read_csv(data_assets['lookup'])

    mastertable = housing_df.merge(lookup_df, on="data_id")
    
    mastertable.drop(columns=['data_id'], inplace=True)
    
    output_dict = {
        "mastertable": mastertable
    }

    save_dataframes(
        dataframes=output_dict,
        data_assets=output_data,
        is_local=is_local,
        save_type="csv"
    )