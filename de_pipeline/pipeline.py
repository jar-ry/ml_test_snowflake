from de_pipeline.nodes.preprocess_data import preprocess_data
from de_pipeline.nodes.process_data import process_data

def run_de_pipeline(is_local):
    """
        Orchestrate and run ML pipeline
    """
    preprocess_data(
        input_data=["housing"],
        output_data=["processed_housing"],
        is_local=is_local
    )
    process_data(
        input_data=["processed_housing", "lookup"],
        output_data=["mastertable"],
        is_local=is_local
    )