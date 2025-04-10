# %%
import pandas as pd
from sklearn.datasets import fetch_california_housing

# %%
main_data_path = "data/01_raw/housing_main.csv"
lookup_data_path = "data/01_raw/housing_lookup.csv"

# %%
housing = fetch_california_housing()
# %%
data=pd.DataFrame(housing['data'],columns=housing['feature_names'])
data['MedHouseVal']=housing['target']

data["data_id"] = data['Latitude'] + data['Longitude'] + data['Population'] + data['AveOccup']
# %%
df_2 = data.copy()
df_2.drop(columns=["HouseAge", "AveRooms", "Latitude",'Longitude','Population','AveOccup', 'AveBedrms']).to_csv(main_data_path, index=False)
data.drop(columns=["MedInc", "MedHouseVal"]).to_csv(lookup_data_path, index=False)
# %%
master_df = pd.read_csv(main_data_path)
lookup_df = pd.read_csv(lookup_data_path)
# %%
master_df.head(3)

# %%
lookup_df.head(3)

# %%
joined_df = master_df.merge(lookup_df, on="data_id")

joined_df
