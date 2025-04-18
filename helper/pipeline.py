from snowflake.snowpark import Session
from pathlib import Path
import importlib.util
import os

class SnowflakePipelineBuilder:
    def __init__(self, session: Session, base_path: str, warehouse: str):
        self.session = session
        self.base_path = Path(base_path)
        self.warehouse = warehouse

    def build_tasks(self, pipeline_name: str, pipeline_definition: dict):
        for node_name, config in pipeline_definition.items():
            sproc_name = f"{pipeline_name}_{config['function']}_sproc"
            after_clause = ""
            if config.get("depends_on"):
                after_clause = "AFTER " + ", ".join(f"task_{pipeline_name}_{dep}" for dep in config["depends_on"])

            sql = f"""
            CREATE OR REPLACE TASK task_{pipeline_name}_{node_name}
            WAREHOUSE = {self.warehouse}
            {after_clause}
            AS
            CALL {sproc_name}();
            """
            self.session.sql(sql).collect()
            print(f"Created task: task_{pipeline_name}_{node_name}")