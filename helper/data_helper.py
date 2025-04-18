import os
import yaml
import pandas as pd
from helper.snowflake_data_helper import SnowflakeDataHelper
from pathlib import Path
from typing import Any, Dict, Union, List


def _get_data_catalogue(data_catalogue_file: str = "data_catalogue.yml") -> dict:
    """
    Get data catalogue configuration as dictionary

    Args:
        data_catalogue_file (str, optional): Data catalogue file name. Defaults to "data_catalogue.yml".

    Returns:
        dict: dictionary values of the data catalogue
    """
    # Path to YAML
    current_dir = Path(__file__).resolve().parent
    yaml_file_path = current_dir / ".." / "conf" / data_catalogue_file
    yaml_file_path = yaml_file_path.resolve()  # resolves symlinks and makes it absolute


    # Open YAML file and load
    with open(yaml_file_path, "r") as yaml_file:
        data_catalogue = yaml.safe_load(yaml_file)
    return data_catalogue


def map_data_assets(data_assets: list[str], **kwargs) -> dict:
    """
    Process list of data assets and return data asset meta data

    Args:
        data_assets (list): List of data asset names in catalogue

    Returns:
        dict: Dictionary of data assets and meta data
    """
    yaml_data = _get_data_catalogue(**kwargs)
    
    data_dict = {}
    
    for data_asset in data_assets:
        data_dict[data_asset] = yaml_data[data_asset]
    
    return data_dict

def save_dataframes(
    dataframes: dict[str, pd.DataFrame],
    data_assets: list[str],
    is_local: bool,
    sf_helper: SnowflakeDataHelper
):
    """
    Save dataframe using data asset name

    Args:
        dataframes (dict[str, pd.DataFrame]): Dictionary of data asset and dataframe objects
        data_assets (list[str]): List of data asset
        is_local (bool): Flag to indictate if job is running locally
        save_type (str, optional): Format to save the dataframe. Defaults to "csv".
    """
    assets_details = map_data_assets(data_assets)

    if not is_local and sf_helper is None:
        raise ValueError("SnowflakeDataHelper must be provided for Snowflake save.")

    for asset_name, df in dataframes.items():
        asset_info = assets_details[asset_name]
        file_type = asset_info.get("file_type", "csv")
        is_folder = asset_info.get("is_folder", False)
        table_name = asset_info.get("table_name", None)
        
        local_path = Path(asset_info["local_path"])
        target_file = asset_info["target_path"]

        if is_folder:   
            local_path.mkdir(parents=True, exist_ok=True)
            local_path = local_path / f"{asset_name}.{file_type}"
            
            # Handle folder logic: maybe upload to @stage/folder/filename.csv
            target_file = f"{target_file.rstrip('/')}/{asset_name}.{file_type}"
        else:
            local_path.parent.mkdir(parents=True, exist_ok=True)
        
        sf_helper.save_dataframe(
            data = df,
            local_path=local_path,
            stage_path=target_file,
            file_type=file_type,
            table_name=table_name
        )


def get_data_reference(
    asset_details: dict,
    sf_helper: SnowflakeDataHelper,
    is_local: bool,
    file_type: str = "csv"
) -> Union[Path, List[Path], str]:
    """
    Resolve path to data asset depending on execution mode (local or Snowflake task).

    Args:
        asset_details (dict): Contains 'local_path', 'target_path', and optionally 'is_folder'.
        sf_helper (SnowflakeDataHelper): Instance of the helper with Snowflake session.
        is_local (bool): Flag to indicate if running locally.
        file_type (str): File format to look for (e.g., 'csv', 'parquet').

    Returns:
        Union[Path, List[Path], str]: Local path(s) or Snowflake path string.
    """
    print(asset_details)
    local_path = Path(asset_details["local_path"])
    target_path = asset_details["target_path"]
    is_folder = asset_details.get("is_folder", False)

    if is_local:
        if is_folder:
            if not local_path.exists() or not any(local_path.glob(f"*.{file_type}")):
                local_path.mkdir(parents=True, exist_ok=True)
                sf_helper._snowflake_session.file.get(target_path, str(local_path))
            return list(local_path.glob(f"*.{file_type}"))
        else:
            if not local_path.exists():
                local_path.parent.mkdir(parents=True, exist_ok=True)
                sf_helper._snowflake_session.file.get(target_path, str(local_path.parent))
            return local_path
    else:
        return target_path  # Use directly in Snowpark (e.g. session.read.csv(path))