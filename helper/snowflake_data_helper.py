from typing import Union
import snowflake.snowpark as sp
from snowflake.snowpark import DataFrame as SPDataFrame
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class SnowflakeDataHelper:
    def __init__(self, session: sp.Session):
        self._session = session

    @property
    def _snowflake_session(self) -> sp.Session:
        return self._session

    def _extract_stage_name(self, stage_path: str) -> str:
        if not stage_path.startswith("@"):
            raise ValueError("Snowflake path must start with '@' for a named stage.")
        return stage_path.split("/")[0].strip("@")  # e.g., '@my_stage'

    def _ensure_stage_exists(self, stage_path: str) -> None:
        stage_name = self._extract_stage_name(stage_path)
        
        try:
            logger.debug(f"Ensuring stage {stage_name} exists...")
            result = self._snowflake_session.sql(
                f"SHOW STAGES LIKE '{stage_name.lstrip('@')}'"
            ).collect()
            if not result:
                logger.info(f"Creating stage {stage_name}...")
                print(f"CREATE STAGE {stage_name}")
                self._snowflake_session.sql(
                    f"CREATE STAGE {stage_name}"
                ).collect()
            else:
                logger.debug(f"Stage {stage_name} already exists.")
        except Exception as e:
            logger.error(f"Error checking/creating stage: {e}")
            raise

    def save_file_to_stage(self, local_path: str, stage_path: str) -> None:
        self._ensure_stage_exists(stage_path)
        self._snowflake_session.file.put(
            local_path,
            stage_path,
            auto_compress=False,
            overwrite=True
        )

    def save_dataframe(
        self, 
        data: Union[pd.DataFrame, SPDataFrame], 
        local_path: Union[str, Path], 
        stage_path: str, 
        file_type: str = "csv", 
        table_name: str = None
    ) -> None:
        local_path = Path(local_path)

        self._ensure_stage_exists(stage_path)

        if isinstance(data, pd.DataFrame):
            # Use in-memory temp file path
            temp_path = Path(f"/tmp/temp_upload.{file_type}")
            if file_type == "csv":
                data.to_csv(temp_path, index=False)
            elif file_type == "parquet":
                data.to_parquet(temp_path, index=False)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            self._snowflake_session.file.put(
                str(temp_path), stage_path, auto_compress=False, overwrite=True
            )

            if table_name:
                # Load the file into a Snowflake table
                format_type_options = (
                    "TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"'"
                    if file_type == "csv"
                    else "TYPE=PARQUET"
                )
                self._snowflake_session.sql(f"""
                    COPY INTO {table_name}
                    FROM @{stage_path}/{temp_path.name}
                    FILE_FORMAT = ({format_type_options})
                    ON_ERROR = 'CONTINUE'
                """).collect()

        elif isinstance(data, SPDataFrame):
            # Save Snowpark DataFrame to stage
            if file_type == "csv":
                data.write.copy_into_location(
                    stage_path, 
                    file_format_type="csv", # In production system use "file_format_name" and create a named file format
                    format_type_options={'COMPRESSION':'None'},
                    overwrite=True, 
                )
            elif file_type == "parquet":
                data.write.copy_into_location(
                    stage_path, 
                    file_format_type="parquet", # In production system use "file_format_name" and create a named file format
                    format_type_options={'COMPRESSION':'None'},
                    overwrite=True, 
                )

            if table_name:
                # Load Snowpark DataFrame into Snowflake table
                data.write.save_as_table(table_name, mode="overwrite")

        # Download from stage to local
        # self._snowflake_session.file.get(stage_path, str(local_path.parent))
        # print(f"Downloaded to local: {local_path.parent}")


    def load_file(self, local_path: Union[str, Path], stage_path: str, file_type: str = "csv") -> pd.DataFrame:
        local_path = Path(local_path)
        if not local_path.parent.exists():
            local_path.parent.mkdir(parents=True, exist_ok=True)

        self._snowflake_session.file.get(
            stage_path,
            str(local_path.parent.absolute())
        )

        if file_type == "csv":
            return pd.read_csv(local_path)
        elif file_type == "parquet":
            return pd.read_parquet(local_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
