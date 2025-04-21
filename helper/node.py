from snowflake.snowpark import Session
from pathlib import Path
import zipfile
import os
import yaml
from typing import List

class SnowflakeNodeBuilder:
    def __init__(self, session: Session, stage: str = "@my_stage"):
        self.session = session
        self.stage = stage

    def register_node(self, func, name, database, schema):
        def wrapper(session: Session, input_data: list, output_data: list, is_local: bool = False) -> str:
            func(session, input_data, output_data, is_local)
            return "OK"

        self._upload_dependencies_to_stage()
        self.session.sproc.register(
            func=wrapper,
            name=name,
            stage_location=self.stage,
            is_permanent=True,
            replace=True,
            packages=[self._get_snowpark_package_version()],
            imports=self._generate_imports_for_sproc(),
            database=database,
            schema=schema
        )
        print(f"Registered stored procedure: {name}")
        
        task_name = f"task_{name}"
        sql = f"""
        CREATE OR REPLACE TASK {task_name}
        WAREHOUSE = COMPUTE_WH
        AFTER KEDRO.PUBLIC.DEFAULT_START_TASK
        AS CALL {database}.{schema}.{name}();
        """
        self.session.sql(sql).collect()
        
        # Check status before resuming
        status_result = self.session.sql(
            f"SHOW TASKS LIKE '{task_name}' IN SCHEMA {database}.{schema}"
        ).collect()
        if status_result and status_result[0]["state"] == "SUSPENDED":
            self.session.sql(f"ALTER TASK {task_name} RESUME").collect()
            print(f"Resumed task: {task_name}")
        else:
            print(f"Task already active: {task_name}")

    def _generate_imports_for_sproc(self) -> List[str]:
        stage_path = Path(".snowflake_dependency")
        return [f"{self.stage}/{f.name}" for f in stage_path.glob("*") if f.is_file()]

    def _upload_dependencies_to_stage(self):
        stage_path = Path(".snowflake_dependency")
        if not stage_path.exists():
            stage_path.mkdir()

        # Dependencies to zip
        dependencies = ["helper", "de_pipeline", 'conf']
        for dep in dependencies:
            exclude_dirs = ["local"] if dep == "conf" else []
            self._compress_folder_to_zip(Path(dep), stage_path / f"{dep}.zip", exclude_dirs=exclude_dirs)

        for file in stage_path.glob("*"):
            self.session.file.put(
                str(file),
                self.stage,
                auto_compress=False,
                overwrite=True
            )

    def _compress_folder_to_zip(self, path: Path, zip_path: Path, exclude=None, exclude_dirs=None):
        exclude = exclude or [".pyc", "__pycache__"]
        exclude_dirs = exclude_dirs or []

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zip_file:
            for root, dirs, files in os.walk(path):
                # Skip excluded directories
                if any(Path(root).resolve().as_posix().startswith(Path(path / ed).resolve().as_posix()) for ed in exclude_dirs):
                    continue

                for file in files:
                    if not any(file.endswith(pattern) for pattern in exclude):
                        file_path = Path(root) / file
                        arcname = os.path.join(path.name, os.path.relpath(file_path, path))
                        zip_file.write(file_path, arcname=arcname)

    def _get_snowpark_package_version(self) -> str:
        env_path = Path("conda.yml")
        if not env_path.exists():
            return "snowflake-snowpark-python"  # fallback

        with open(env_path, "r") as file:
            env = yaml.safe_load(file)
            for dep in env.get("dependencies", []):
                if isinstance(dep, str) and dep.startswith("snowflake-snowpark-python"):
                    return dep
                elif isinstance(dep, dict) and "pip" in dep:
                    for pip_dep in dep["pip"]:
                        if pip_dep.startswith("snowflake-snowpark-python"):
                            return pip_dep

        return "snowflake-snowpark-python"

    def _ensure_dummy_start_task(self):
        self.session.sql("""
            CREATE OR REPLACE PROCEDURE DEFAULT_START()
            RETURNS STRING
            LANGUAGE SQL
            AS
            $$
                SELECT 'Start';
            $$;
        """).collect()

        self.session.sql("""
            CREATE OR REPLACE TASK KEDRO.PUBLIC.DEFAULT_START_TASK
            WAREHOUSE = COMPUTE_WH
            SCHEDULE = '11520 MINUTE'
            AS CALL DEFAULT_START();
        """).collect()

        print("Ensured dummy start task exists: DEFAULT_START_TASK")