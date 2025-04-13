import pandas as pd
from helper.data_helper import map_data_assets, get_data_reference, save_dataframes
from helper.snowflake_connect_manager import SnowflakeConnectionManager
from helper.snowflake_data_helper import SnowflakeDataHelper

def preprocess_data(input_data: list[str], output_data: list[str], is_local) -> pd.DataFrame:
    asset_paths = map_data_assets(input_data)

    conn_mgr = SnowflakeConnectionManager()
    session = conn_mgr.create_session()
    sf_helper = SnowflakeDataHelper(session)

    # Resolve the reference to the data (local path or Snowflake stage path)
    housing_ref = get_data_reference(asset_paths["housing"], sf_helper, is_local)

    if is_local:
        housing_df = pd.read_csv(housing_ref)
    else:
        try:
            housing_df = session.read.option("header", True).csv(housing_ref)
            print(housing_df.head())
        except Exception as e:
            print(f"Snowflake read failed: {e}")
            print("Attempting to upload local file to Snowflake...")

            # Upload local file (you can extend this to support folders too)
            local_path = asset_paths["housing"]["local_path"]
            sf_helper.save_file_to_stage(local_path, housing_ref)

            # Retry reading after upload
            housing_df = session.read.option("header", True).csv(housing_ref)
    
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