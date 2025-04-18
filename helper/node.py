from snowflake.snowpark import Session
from pathlib import Path
import shutil
import zipfile
import importlib.util
import os
import sys
import yaml
from typing import List

class SnowflakeNodeBuilder:
    def __init__(self, session: Session, stage: str = "@my_stage"):
        self.session = session
        self.stage = stage

    def register_node(self, func, name, database, schema):
        def wrapper(session: Session, input_data: list, output_data: list, is_local: bool = False) -> str:
            # Dynamically import the function to ensure it's in scope
            module_name = "de_pipeline.nodes.preprocess_data"
            module = importlib.import_module(module_name)
            func = getattr(module, 'preprocess_data')  # Replace with actual function name if different

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

    def _generate_imports_for_sproc(self) -> List[str]:
        stage_path = Path(".snowflake_dependency")
        return [f"{self.stage}/{f.name}" for f in stage_path.glob("*") if f.is_file()]

    def _upload_dependencies_to_stage(self):
        stage_path = Path(".snowflake_dependency")
        if not stage_path.exists():
            stage_path.mkdir()

        # Dependencies to zip
        dependencies = ["helper", "de_pipeline"]
        for dep in dependencies:
            self._compress_folder_to_zip(Path(dep), stage_path / f"{dep}.zip")

        for file in stage_path.glob("*"):
            self.session.file.put(
                str(file),
                self.stage,
                auto_compress=False,
                overwrite=True
            )

    def _compress_folder_to_zip(self, path: Path, zip_path: Path, exclude=None):
        exclude = exclude or [".pyc", "__pycache__"]
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zip_file:
            for root, dirs, files in os.walk(path):
                for file in files:
                    if not any(file.endswith(pattern) for pattern in exclude):
                        file_path = os.path.join(root, file)
                        zip_file.write(file_path, arcname=os.path.relpath(file_path, path))

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
