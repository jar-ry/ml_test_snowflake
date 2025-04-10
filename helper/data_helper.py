import os
import yaml
import pandas as pd
from pathlib import Path

def _get_data_catalogue(data_catalogue_file: str = "data_catalogue.yml") -> dict:
    """
    Get data catalogue configuration as dictionary

    Args:
        data_catalogue_file (str, optional): Data catalogue file name. Defaults to "data_catalogue.yml".

    Returns:
        dict: dictionary values of the data catalogue
    """
    # Path to YAML
    yaml_file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        f"../conf/{data_catalogue_file}"
    )

    # Open YAML file and load
    with open(yaml_file_path, "r") as yaml_file:
        data_catalogue = yaml.safe_load(yaml_file)
    return data_catalogue


def _map_data_assets(data_assets: list[str], **kwargs) -> dict:
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

def get_data_path(data_assets: list[str], is_local, **kwargs) -> dict:
    """
    Process list of data assets and return data asset meta data

    Args:
        data_assets (list[str]): List of data asset names in catalogue
        is_local (bool): Flag to indictate if job is running locally
    Returns:
        dict: Dictionary of data assets and meta data
    """
    assets_details = _map_data_assets(data_assets, **kwargs)
    if is_local:
        # TODO download datasets to local compute (if it doesn't exist)
        mapped_catalog = {key: value["local_path"] for key, value in assets_details.items()}
    else:
        # TODO map to deployment path
        mapped_catalog = {key: value["local_path"] for key, value in assets_details.items()}
    
    return mapped_catalog

def save_dataframes(
    dataframes: dict[str, pd.DataFrame],
    data_assets: list[str],
    is_local: bool,
    save_type: str = "csv"
):
    """
    Save dataframe using data asset name

    Args:
        dataframes (dict[str, pd.DataFrame]): Dictionary of data asset and dataframe objects
        data_assets (list[str]): List of data asset
        is_local (bool): Flag to indictate if job is running locally
        save_type (str, optional): Format to save the dataframe. Defaults to "csv".
    """
    assets_details = get_data_path(data_assets, is_local)
     
    for data_asset, dataframe in dataframes.items():
        save_path = assets_details[data_asset]
        if Path(save_path).is_dir():
            if not Path(save_path).exists():
                Path(save_path).mkdir(parents=True, exist_ok=True)
        else:
            dir_path = "/".join(save_path.split("/")[:-1])
            if not Path(dir_path).exists():
                Path(dir_path).mkdir(parents=True, exist_ok=True)
        if save_type == "csv":
            if isinstance(dataframe, pd.DataFrame) or isinstance(dataframe, pd.Series):
                dataframe.to_csv(save_path)
        elif save_type == "parquet":
            if isinstance(dataframe, pd.DataFrame) or isinstance(dataframe, pd.Series):
                dataframe.to_parquet(save_path)