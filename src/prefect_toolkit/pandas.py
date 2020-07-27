from pathlib import Path
from typing import List, Dict

from functools import reduce
import pandas as pd
import numpy as np
from prefect import task


# Input/Output
# ************


@task
def load_csv_to_dataframe(filepath: Path) -> pd.DataFrame:

    return pd.read_csv(filepath)


@task
def load_excel_to_dataframe(filepath: Path, sheet_name: str) -> pd.DataFrame:

    return pd.read_excel(filepath, sheet_name)


# Run even if preceding task (such as downloading) is skipped!
@task(skip_on_upstream_skip=False)
def load_parquet_to_dataframe(filepath: Path) -> pd.DataFrame:

    return pd.read_parquet(filepath)


@task
def save_dataframe_to_parquet(df: pd.DataFrame, filepath: Path) -> None:

    df.to_parquet(filepath)


@task
def read_all_excel_sheets_into_sheet_to_df_map(
    filename: Path,
) -> Dict[str, pd.DataFrame]:

    return pd.read_excel(filename, sheet_name=None)


# Formatting
# **********


@task
def rename_dataframe_columns(df: pd.DataFrame, columns: Dict[str, str]) -> pd.DataFrame:

    return df.rename(columns=columns)


@task
def set_column_names_lowercase(df: pd.DataFrame) -> pd.DataFrame:

    return df.rename(columns=str.lower)


@task
def strip_whitespace_from_column_names(df: pd.DataFrame) -> pd.DataFrame:

    return df.rename(columns=str.strip)


@task
def set_column_strings_lowercase(df: pd.DataFrame, column: str) -> pd.DataFrame:

    df.loc[:, column] = df[column].str.lower()
    return df


# Indexing
# ********


@task
def set_dataframe_index(df: pd.DataFrame, index_column: str) -> pd.DataFrame:

    return df.set_index(index_column)


@task
def reset_dataframe_index(df: pd.DataFrame) -> pd.DataFrame:

    return df.reset_index()


# Mutable Operations
# ******************


@task
def extract_columns_from_dataframe(
    df: pd.DataFrame, columns: List[str]
) -> pd.DataFrame:

    return df[columns]


@task
def drop_rows_where_column_value_is_in_list(
    df: pd.DataFrame, column: str, values: List[str]
) -> pd.DataFrame:

    mask = np.isin(df["dwelling_type"], ["bed-sit", "caravan/mobile home"])
    return df[~mask]


@task
def split_into_n_dataframes(df: pd.DataFrame, n: int) -> pd.DataFrame:

    return np.split(df, n, axis=1)


@task
def add_empty_named_columns_to_dataframe(df: pd.DataFrame, columns: List[str]):

    return pd.concat([df, pd.DataFrame(columns=columns)])


@task
def add_id_column_based_on_unique_column_values(
    df: pd.DataFrame, column: str, id_column: str
) -> pd.DataFrame:

    df[id_column] = pd.util.hash_pandas_object(df[column], index=False)
    return df


@task
def extract_rows_matching_condition_in_column(df: pd.DataFrame, query: str):

    column, comparison, against = query.split(" ")

    return df.query(f"{column} {comparison} {against}")


@task
def merge_dataframes(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str = None,
    left_on: str = None,
    right_on: str = None,
) -> pd.DataFrame:

    return pd.merge(left, right, on, left_on, right_on)


# Type Conversion
# ***************


@task
def infer_dataframe_data_types(df: pd.DataFrame) -> pd.DataFrame:

    return df.convert_dtypes()

