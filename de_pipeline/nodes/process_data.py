import pandas as pd
from helper.data_helper import map_data_assets, get_data_reference, save_dataframes
from helper.snowflake_data_helper import SnowflakeDataHelper
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, IntegerType, FloatType
from snowflake.snowpark.functions import col, round

# Define the schema for CSV
processed_housing_schema = StructType([
    StructField("HouseAge", FloatType()),
    StructField("AveRooms", FloatType()),
    StructField("AveBedrms", FloatType()),
    StructField("Population", FloatType()),
    StructField("AveOccup", FloatType()),
    StructField("Latitude", FloatType()),
    StructField("Longitude", FloatType()),
    StructField("data_id", FloatType())
])

lookup_schema = StructType([
    StructField("MedInc", FloatType()),
    StructField("MedHouseVal", FloatType()),
    StructField("data_id", FloatType())
])


def process_data(session: Session, input_data: list[str], output_data: list[str], is_local) -> pd.DataFrame:
    asset_paths = map_data_assets(input_data)

    sf_helper = SnowflakeDataHelper(session)

    # Resolve the reference to the data (local path or Snowflake stage path)
    housing_ref = get_data_reference(asset_paths["processed_housing"], sf_helper, is_local)
    lookup_ref = get_data_reference(asset_paths["lookup"], sf_helper, is_local)

    if is_local:
        # housing_df = pd.read_csv(housing_ref)
        housing_df = session.read.schema(processed_housing_schema).csv(housing_ref)
        lookup_df = session.read.schema(lookup_schema).csv(lookup_ref)
    else:
        try:
            housing_df = session.read.schema(processed_housing_schema).csv(housing_ref)
            lookup_df = session.read.schema(lookup_schema).csv(lookup_ref)
        except Exception as e:
            print(f"Snowflake read failed: {e}")
            print("Attempting to upload local file to Snowflake...")

            # Upload local file
            local_path = asset_paths["lookup"]["local_path"]
            sf_helper.save_file_to_stage(local_path, lookup_ref)

            # Retry reading after upload
            lookup_df = session.read.schema(lookup_schema).csv(lookup_ref)

    mastertable = housing_df.join(lookup_df, on="data_id")
    
    mastertable = mastertable.drop('data_id')
    
    output_dict = {
        "mastertable": mastertable
    }

    save_dataframes(
        dataframes=output_dict,
        data_assets=output_data,
        is_local=is_local,
        sf_helper=sf_helper
    )