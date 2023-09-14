"""
Pandas Validation Utility

This module provides a utility class, PandasValidations,
for performing data validations on Pandas DataFrames using Great Expectations.

The PandasValidations class allows users to create an expectation suite, define expectations,
and validate data in a Pandas DataFrame. It can be used as a context manager to simplify the validation process.

Classes:
    PandasValidations: A class for creating and managing data validations for Pandas DataFrames.

Example Usage:
    # Import the PandasValidations class
    from validations.PandasValidation import PandasValidations

    # Create an expectation suite and perform data validation
    with PandasValidations(expectation_suite_name="my_expectations", df=my_dataframe) as validator:
        validator.expect_column_values_to_be_in_set(column="Age", value_set=[18, 25, 30])
        validator.expect_column_mean_to_be_between(column="Salary", min_value=50000, max_value=100000)

    # When exiting the context, the expectation suite is deleted.

Note:
- This module requires the Great Expectations library (great_expectations) to be installed.
- It assumes the presence of a Utils module for logging purposes.
- Users can specify either a DataFrame (df) or a file path and file type (e.g., "parquet" or "csv") for validation.

"""

from typing import Optional

import great_expectations as gx
import pandas as pd
from great_expectations.validator.validator import Validator
from utils import Utils


class PandasValidations:
    def __init__(
        self,
        expectation_suite_name: str,
        df: Optional[pd.DataFrame] = None,
        file_path: Optional[str] = None,
        file_type: Optional[str] = None,
    ) -> None:
        if df is None and (file_path is None or file_type is None):
            raise ValueError("Either df or file_path and file_type must be provided")
        if file_type not in ["parquet", "csv"]:
            raise ValueError("file_type must be either parquet or csv")
        if df is None:
            if file_type == "parquet":
                self.df = pd.read_parquet(file_path)
            else:
                self.df = pd.read_csv(file_path)
        else:
            self.df = df
        Utils.logger.info(f"Succefully read dataframe. Shape: {self.df.shape}")
        self.gx_suite_name = expectation_suite_name

    def __enter__(self) -> Validator:
        self.context = gx.get_context()
        self.context.add_or_update_expectation_suite(self.gx_suite_name)
        datasource = self.context.sources.add_pandas(name="pandas_datasource")
        data_asset = datasource.add_dataframe_asset(name="df_asset")
        return self.context.get_validator(
            batch_request=data_asset.build_batch_request(dataframe=self.df),
            expectation_suite_name=self.gx_suite_name,
        )

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """This will delete the the expectation suite."""
        self.context.delete_expectation_suite(self.gx_suite_name)
