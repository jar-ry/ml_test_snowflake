import yaml
import pandas as pd
from helper.snowflake_data_helper import SnowflakeDataHelper
from snowflake.snowpark import DataFrame as SPDataFrame

from pathlib import Path
from typing import Union, List
import pkgutil


def _get_data_catalogue(data_catalogue_file: str = "data_catalogue.yml") -> dict:
    """
    Load data catalogue YAML, supporting both local and Snowflake sproc execution.
    """
    # Option 1: Local execution path (actual file system)
    local_paths = [
        Path(__file__).resolve().parent / ".." / "conf" / data_catalogue_file,
    ]

    for path in local_paths:
        try:
            path = path.resolve()
            if path.exists():
                with open(path, "r") as f:
                    return yaml.safe_load(f)
        except Exception:
            pass  # If path can't resolve (e.g. ZIP context), move on

    # Option 2: Sproc execution via zipimport — try reading as resource
    try:
        yaml_bytes = pkgutil.get_data("conf", data_catalogue_file)
        if yaml_bytes is not None:
            return yaml.safe_load(yaml_bytes.decode("utf-8"))
    except Exception:
        pass

    raise FileNotFoundError(f"Could not find {data_catalogue_file} in local paths or package resources.")


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
    dataframes: dict[str, Union[pd.DataFrame, SPDataFrame]],
    data_assets: list[str],
    is_local: bool,
    sf_helper: SnowflakeDataHelper
):
    """
    Save dataframe using data asset name

    Args:
        dataframes (dict[str, DataFrame]): Dictionary of asset names to dataframes.
        data_assets (list[str]): List of asset names to save.
        is_local (bool): Whether running locally or in Snowflake.
        sf_helper (SnowflakeDataHelper): Required when not local.
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

        if is_local:
            # Local save
            if is_folder:
                local_path.mkdir(parents=True, exist_ok=True)
                local_path = local_path / f"{asset_name}.{file_type}"
            else:
                local_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(local_path, index=False) if file_type == "csv" else df.to_parquet(local_path, index=False)

        else:
            # Snowflake save
            if is_folder:
                target_file = f"{target_file.rstrip('/')}/{asset_name}.{file_type}"
            sf_helper.save_dataframe(
                data=df,
                local_path=local_path,  # Still needed for Pandas temp file in Snowflake
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