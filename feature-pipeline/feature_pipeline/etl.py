import subprocess
import numpy as np
import hopsworks
import pandas as pd
from pandas.errors import EmptyDataError
from typing import Any, Dict, Tuple, Optional
from great_expectations.core import ExpectationSuite
from hsfs.feature_group import FeatureGroup
from pathlib import Path

from feature_pipeline.settings import SETTINGS
from feature_pipeline import utils, settings

logger = utils.get_logger(__name__)

class Extract:
    def __init__(self):
        self.logger = logger

    def from_file(
            self,
            data_path: str = "carrie1/ecommerce-data",
            cache_dir: Optional[Path] = None,
    ) -> Optional[Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Extract E-commerce dataset from Kaggle.
        Args:
            data_path: Kaggle data path 
            cache_dir: The directory where the downloaded data will be cached. By default it will be downloaded in the standard output directory.
        Returns:
            A tuple of a Pandas DataFrame containing the exported data and a dictionary of metadata.
        """

        records = self._extract_records_from_file(data_path = data_path, cache_dir=cache_dir)
        
        metadata = {
            "data_path": data_path,
            "num_unique_samples_per_time_series": len(records["InvoiceDate"].unique()),
        }

        return records, metadata


    def _extract_records_from_file(self, data_path: str, cache_dir: Optional[Path] = None) -> Optional[pd.DataFrame]:
        """Extract records from the file backup based on the given export window."""

        if cache_dir is None:
            cache_dir = settings.OUTPUT_DIR / "data"
            cache_dir.mkdir(parents=True, exist_ok=True)
            
        file_path = cache_dir / "data.csv"
        if not file_path.exists():
            logger.info(f"Downloading data from: {data_path}")

            try:
                # Download data
                subprocess.run(f"kaggle datasets download -d carrie1/ecommerce-data -p {cache_dir} --unzip")

                # Unzip file
                subprocess
            except Exception as e:
                logger.error(
                    f"Could not download the file due to: {e}"
                )

                return None

            logger.info(f"Successfully downloaded data to: {file_path}")
        else:
            logger.info(f"Data already downloaded at: {file_path}")

        try:
            data = pd.read_csv(file_path, encoding='unicode_escape')
        except EmptyDataError:
            file_path.unlink(missing_ok=True)
            
            raise ValueError(f"Downloaded file at {file_path} is empty. Could not load it into a DataFrame.")
        
        return data

class Transform:
    def __init__(self, df:pd.DataFrame):
        self.logger = logger
        self.data = df

    def rename_columns(self) -> pd.DataFrame:
        """
        Rename columns to match our schema.
        """
        # Drop irrelevant columns.
        self.data.drop(columns=["StockCode", "Description", "CustomerID", "Quantity"], inplace=True)

        # Rename columns
        self.data.rename(
            columns={
                "InvoiceNo": "invoice_id",
                "InvoiceDate": "invoice_date",
                "UnitPrice": "total_price",
                "Country":"country"
            },
            inplace=True,
        )

        return self.data
    
    def cast_columns(self) -> pd.DataFrame:
        """
        Cast columns to the correct data type.
        """

        self.data["invoice_date"] = pd.to_datetime(self.data["invoice_date"], format = '%m/%d/%Y %H:%M')

    def filter_countries(self, country_list:list):
        """
        Filter data on given country_list
        """
        self.data = self.data[self.data['country'].isin(country_list)].copy()
        self.country_mappings = {v:i for i, v in enumerate(self.data['country'].unique())}
    
    def encode_country_column(self) -> pd.DataFrame:
        """
        Encode the country column to integers.
        """
        self.data["country"] = self.data["country"].map(lambda string_area: self.country_mappings.get(string_area))
        self.data["country"] = self.data["country"].astype("int8")

        return self.data
    
    def aggregate_data(self) -> pd.DataFrame:
        """
        Do aggregation based on country and daily data
        """
        # Aggregation
        agg_data = self.data.groupby(['invoice_date', 'invoice_id', 'country']).agg({'total_price':'sum'}).reset_index()

        # Combine 
        all_agg_data = pd.DataFrame()

        for country in [0, 1, 2]:
            # Filter on 1 country
            agg_data_1 = agg_data[agg_data['country'] == country].groupby(['invoice_date']).agg({'total_price':'sum'}).reset_index()

            # Remove outliers
            agg_data_non_outliers = self.iqr_outlier_removal(agg_data_1, agg_col = 'total_price')

            # Resample Daily
            agg_data_non_outliers = agg_data_non_outliers.resample('D', on = 'invoice_date').sum().interpolate().round().reset_index()
            
            # Add country column
            agg_data_non_outliers['country'] = country

            # Concat data
            all_agg_data = pd.concat([all_agg_data, agg_data_non_outliers], axis = 0, ignore_index = True)
        
        # Add ID column
        all_agg_data['id'] = [i for i in range(all_agg_data.shape[0])]
        
        # Return all agg data
        return all_agg_data

    # Remove extreme outliers
    def iqr_outlier_removal(self, agg_data:pd.DataFrame, agg_col:str):
        '''
        Remove outliers in all aggregate column based on IQR method by replacing
        outliers with the interpolation value
        Input:
            * agg_data -> Aggregated data by salesdate on all aggregate columns
            * agg_cols -> List of aggregate columns
        '''
        # Calculate IQR
        q1, q3 = agg_data[agg_col].quantile([0.25, 0.75])
        iqr = q3 - q1

        # Define lower and upper bound
        lower_bound = q1 - 3 * iqr
        upper_bound = q3 + 3 * iqr

        # Idenfity outliers and mark as NaN
        agg_data.loc[(agg_data[agg_col] < lower_bound) | (agg_data[agg_col] > upper_bound), agg_col] = np.nan

        # Drop null values
        agg_data.dropna(inplace = True)

        return agg_data

class Load:
    def __init__(self):
        self.logger = logger
    
    def to_feature_store(
        self,
        data: pd.DataFrame,
        validation_expectation_suite: ExpectationSuite,
        feature_group_version: int,
    ) -> FeatureGroup:
        """
        This function takes in a pandas DataFrame and a validation expectation suite,
        performs validation on the data using the suite, and then saves the data to a
        feature store in the feature store.
        """

        # Connect to feature store.
        project = hopsworks.login(
            api_key_value=SETTINGS["FS_API_KEY"], project=SETTINGS["FS_PROJECT_NAME"]
        )
        feature_store = project.get_feature_store()

        # Create feature group.
        ecommerce_feature_group = feature_store.get_or_create_feature_group(
            name="e_commerce_data",
            version=feature_group_version,
            description="Online E-commerce data ranging from 2011-2012",
            primary_key=["id"],
            event_time="invoice_date",
            online_enabled=False,
            expectation_suite=validation_expectation_suite,
        )
        # Upload data.
        ecommerce_feature_group.insert(
            features=data,
            overwrite=False,
            write_options={
                "wait_for_job": True,
            },
        )

        # Add feature descriptions.
        feature_descriptions = [
            {
                "name": "id",
                "description": """
                                ID of observation
                                """,
                "validation_rules": ">0 (int)",
            },
            {
                "name": "invoice_date",
                "description": """
                                Datetime interval in UTC when the data was observed.
                                """,
                "validation_rules": "Datetime %Y-%m-%d %H:%M",
            },
            {
                "name": "country",
                "description": """
                                Country's origin
                                """,
                "validation_rules": "0 (UK), 1 (France) or 2 (Germany) (int)",
            },
            {
                "name": "total_price",
                "description": """
                                Total price at that day
                                """,
                "validation_rules": ">0 (float)",
            },
        ]
        for description in feature_descriptions:
            ecommerce_feature_group.update_feature_description(
                description["name"], description["description"]
            )

        # Update statistics.
        ecommerce_feature_group.statistics_config = {
            "enabled": True,
            "histograms": True,
            "correlations": True,
        }
        ecommerce_feature_group.update_statistics_config()
        ecommerce_feature_group.compute_statistics()

        return ecommerce_feature_group