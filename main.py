from de_pipeline.pipeline import run_de_pipeline
from ds_pipeline.pipeline import run_ds_pipeline
import argparse
import logging

def parse_args():
    # Set up arg parser
    parser = argparse.ArgumentParser()
    
    # Add arguemnts
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--debug", action="store_true")
    
    # Parse Args
    return parser.parse_args()
    

if __name__  == "__main__":
    # Process args
    args = parse_args()
    
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        # Suppress DEBUG logs from third-party modules
        for lib in logging.root.manager.loggerDict:
            if not lib.startswith("helper"):
                logging.getLogger(lib).setLevel(logging.WARNING)
        
    run_de_pipeline(
        is_local=args.local
    )

    # run_ds_pipeline(
    #     is_local=args.local
    # )