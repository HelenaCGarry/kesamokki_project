import logging
from src.data_pipeline.cabins_extraction import extract_data
from src.data_pipeline.cabins_transform import transform_data
from src.data_pipeline.cabins_update import update_data

# Configure logging
logging.basicConfig(
    filename='data_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'  # Append mode to keep old logs
)

def main():
    # Run data extraction
    extract_data()
    # Run data transformation
    transform_data()
    # Run data updating
    update_data()

if __name__ == "__main__":
    main()