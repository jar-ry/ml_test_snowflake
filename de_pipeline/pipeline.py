from de_pipeline.nodes.preprocess_data import preprocess_data
from de_pipeline.nodes.process_data import process_data
from helper.node import SnowflakeNodeBuilder
from helper.pipeline import SnowflakePipelineBuilder
from snowflake.snowpark import Session

def register_de_nodes(session: Session):
    node_builder = SnowflakeNodeBuilder(session)
    
    node_builder.register_node(
        func=preprocess_data,
        name="de_preprocess_data_sproc",
        database="MY_DB",
        schema="PUBLIC"
    )

    node_builder.register_node(
        func=process_data,
        name="de_process_data_sproc",
        database="MY_DB",
        schema="PUBLIC"
    )

def run_de_pipeline(session: Session, is_local: bool):
    """
        Orchestrate and run ML pipeline
    """
    pipeline_builder = SnowflakePipelineBuilder(session, ".", "COMPUTE_WH")
    
    pipeline_definition = {
    "preprocess_data": {
        "function": "preprocess_data",
        "depends_on": []
    },
    "process_data": {
        "function": "process_data",
        "depends_on": ["preprocess_data"]
    }
}
    
    pipeline_builder.build_tasks("de", pipeline_definition)
    # preprocess_data(
    #     session=session,
    #     input_data=["housing"],
    #     output_data=["processed_housing"],
    #     is_local=is_local
    # )
    # process_data(
    #     session=session,
    #     input_data=["processed_housing", "lookup"],
    #     output_data=["mastertable"],
    #     is_local=is_local
    # )