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

