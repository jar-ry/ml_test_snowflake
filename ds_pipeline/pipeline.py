from ds_pipeline.nodes.split_data import training_split
from ds_pipeline.nodes.model import train
from ds_pipeline.nodes.evaluate import evaluate

def run_ds_pipeline(is_local):
    """
        Orchestrate and run ML pipeline
    """
    training_split(
        input_data=["mastertable"],
        output_data=["x_train", "y_train", "x_test", "y_test"],
        is_local=is_local
    )

    train(
        input_data=["x_train", "y_train"],
        output_data=["lr_model"],
        is_local=is_local
    )
    
    evaluate(
        input_data=["lr_model", "x_test", "y_test"],
        output_data=["metrics"],
        is_local=is_local
    )
    