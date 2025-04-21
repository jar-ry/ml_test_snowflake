from snowflake.snowpark import Session


class SnowflakePipelineBuilder:
    def __init__(self, session: Session, pipeline_definition: dict, warehouse: str):
        self.session = session
        self.pipeline_definition = pipeline_definition
        self.warehouse = warehouse

    def _serialize_param_dict(self, param_dict):
        serialized = []
        for key in ["input_data", "output_data", "is_local"]:
            value = param_dict.get(key)
            if isinstance(value, list):
                serialized.append(f"ARRAY_CONSTRUCT({', '.join(repr(v) for v in value)})")
            else:
                serialized.append(repr(value))  # For bools or scalars
        return ", ".join(serialized)

    def build_tasks(self, pipeline_name: str):
        self.build_dummy_start_task(pipeline_name=pipeline_name)

        # Create actual pipeline tasks
        for node_name, config in self.pipeline_definition.items():
            sproc_name = f"{pipeline_name}_{config['function']}_sproc"
            after_clause = ""

            if config.get("depends_on"):
                after_clause = "AFTER " + ", ".join(f"task_{pipeline_name}_{dep}" for dep in config["depends_on"])
            else:
                after_clause = f"AFTER START_{pipeline_name}"

            params_dict = config.get("params", {})
            params_str = self._serialize_param_dict(params_dict)

            sql = f"""
                CREATE OR REPLACE TASK task_{pipeline_name}_{node_name}
                WAREHOUSE = {self.warehouse}
                {after_clause}
                AS
                CALL {sproc_name}({params_str});
            """
            self.session.sql(sql).collect()
            print(f"Created task: task_{pipeline_name}_{node_name}")
    
    
    def run_pipeline(self, pipeline_name: str):
        """
        Trigger root task(s) in the pipeline to begin execution.
        """
        entry_nodes = [node for node, cfg in self.pipeline_definition.items() if not cfg.get("depends_on")]

        try:
            for node in entry_nodes:
                task_name = f"task_{pipeline_name}_{node}"
                sql = f"CALL SYSTEM$TASK_FORCE_RUN('{task_name}');"
                self.session.sql(sql).collect()
                print(f"Triggered task: {task_name}")
        except Exception as e:
            self.session.sql(f"call SYSTEM$TASK_DEPENDENTS_ENABLE('START_{pipeline_name}');").collect()
            self.session.sql(f"alter task START_{pipeline_name} resume;").collect()
            self.session.sql(f"EXECUTE TASK START_{pipeline_name}").collect()
                
                
    def build_dummy_start_task(self, pipeline_name):
        dummy_start_sql = f"""
            CREATE OR REPLACE TASK KEDRO.PUBLIC.START_{pipeline_name}
            WAREHOUSE = {self.warehouse}
            SCHEDULE = '11520 MINUTE'
            AS
            SELECT 1;
            """
        self.session.sql(dummy_start_sql).collect()
        # self.session.sql(f"ALTER TASK START_{pipeline_name} RESUME").collect()
        print(f"Created and resumed dummy task: START_{pipeline_name}")