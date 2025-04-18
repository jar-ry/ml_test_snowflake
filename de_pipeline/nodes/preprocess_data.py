import pandas as pd
from helper.data_helper import map_data_assets, get_data_reference, save_dataframes
from helper.snowflake_data_helper import SnowflakeDataHelper
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, IntegerType, FloatType
from snowflake.snowpark.functions import col, round

# Define the schema for CSV
housing_schema = StructType([
    StructField("HouseAge", FloatType()),
    StructField("AveRooms", FloatType()),
    StructField("AveBedrms", FloatType()),
    StructField("Population", FloatType()),
    StructField("AveOccup", FloatType()),
    StructField("Latitude", FloatType()),
    StructField("Longitude", FloatType()),
    StructField("data_id", FloatType())
])


def preprocess_data(session: Session, input_data: list[str], output_data: list[str], is_local) -> pd.DataFrame:
    asset_paths = map_data_assets(input_data)

    sf_helper = SnowflakeDataHelper(session)

    # Resolve the reference to the data (local path or Snowflake stage path)
    housing_ref = get_data_reference(asset_paths["housing"], sf_helper, is_local)

    if is_local:
        # housing_df = pd.read_csv(housing_ref)
        housing_df = session.read.schema(housing_schema).csv(housing_ref)
    else:
        try:
            housing_df = session.read.schema(housing_schema).csv(housing_ref)
        except Exception as e:
            print(f"Snowflake read failed: {e}")
            print("Attempting to upload local file to Snowflake...")

            # Upload local file
            local_path = asset_paths["housing"]["local_path"]
            sf_helper.save_file_to_stage(local_path, housing_ref)

            # Retry reading after upload
            housing_df = session.read.schema(housing_schema).csv(housing_ref)

    housing_df = housing_df.with_column("AveRooms", round(col("AveRooms"), 2))
    housing_df = housing_df.with_column("AveBedrms", round(col("AveBedrms"), 2))
    housing_df = housing_df.with_column("AveOccup", round(col("AveOccup"), 2))

    output_dict = {
        "processed_housing": housing_df
    }

    save_dataframes(
        dataframes=output_dict,
        data_assets=output_data,
        is_local=is_local,
        sf_helper=sf_helper
    )