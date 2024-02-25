import datetime
from typing import Optional
import fire
import pandas as pd

#from feature_pipeline.etl import Cleaning, load, Extract, validation
from feature_pipeline.etl import Extract, Transform, Load
from feature_pipeline import utils, validation

logger = utils.get_logger(__name__)

def run(
    data_path: str = "carrie1/ecommerce-data",
    feature_group_version: int = 1,
) -> dict:
    """
    Extract E-commerce dataset from Kaggle.
    Args:
        data_path: Kaggle data path 
        cache_dir: The directory where the downloaded data will be cached. By default it will be downloaded in the standard output directory.
        eature_group_version: The version of the feature store feature group to save the data to.
    Returns:
          A dictionary containing metadata of the pipeline.
    """

    logger.info(f"Extracting data from Kaggle.")
    Extractor = Extract()
    data, metadata = Extractor.from_file(
        data_path = data_path,

    )
    logger.info("Successfully extracted data from Kaggle.")

    logger.info(f"Transforming data.")
    data = transform(data)
    logger.info("Successfully transformed data.")

    logger.info("Building validation expectation suite.")
    validation_expectation_suite = validation.build_expectation_suite()
    print(validation_expectation_suite)
    logger.info("Successfully built validation expectation suite.")

    logger.info(f"Validating data and loading it to the feature store.")
    loader = Load()
    loader.to_feature_store(
        data,
        validation_expectation_suite=validation_expectation_suite,
        feature_group_version=feature_group_version,
    )
    metadata["feature_group_version"] = feature_group_version
    logger.info("Successfully validated data and loaded it to the feature store.")

    logger.info(f"Wrapping up the pipeline.")
    utils.save_json(metadata, file_name="feature_pipeline_metadata.json")
    logger.info("Done!")

    return metadata


def transform(data: pd.DataFrame):
    """
    Wrapper containing all the transformations from the ETL pipeline.
    """
    transformer = Transform(df = data)

    # Do transformation
    transformer.rename_columns()
    transformer.cast_columns()
    transformer.filter_countries(country_list = ['United Kingdom', 'France', 'Germany'])
    transformer.encode_country_column()
    data_cleaned = transformer.aggregate_data()

    return data_cleaned

if __name__ == "__main__":
    fire.Fire(run)