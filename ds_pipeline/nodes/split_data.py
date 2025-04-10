import pandas as pd
from helper.data_helper import get_data_path, save_dataframes
from sklearn.model_selection import train_test_split


def training_split(input_data: list[str], output_data: list[str], is_local) -> pd.DataFrame:
    data_assets = get_data_path(input_data, is_local)
    
    housing_df = pd.read_csv(data_assets['mastertable'])
    
    X=housing_df.drop('MedHouseVal',axis=1)
    y=housing_df['MedHouseVal']
    
    X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.33)

    output_dict = {
        "x_train": X_train,
        "x_test": X_test,
        "y_train": y_train,
        "y_test": y_test,
    }

    save_dataframes(
        dataframes=output_dict,
        data_assets=output_data,
        is_local=is_local,
        save_type="csv"
    )