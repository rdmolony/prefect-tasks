from pathlib import Path

import geopandas as gpd
from prefect import task


@task
def load_geodata_to_geodataframe(filepath: Path) -> gpd.GeoDataFrame:

    return gpd.read_file(filepath)
