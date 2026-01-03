from dataclasses import dataclass
from typing import Dict

import pandas as pd
from msk_cdm.databricks import DatabricksAPI


@dataclass
class DatabricksTableConfig:
    catalog: str
    schema: str
    table: str
    volume_path: str
    sep: str = "\t"
    overwrite: bool = True

    @property
    def fully_qualified_table(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    def as_table_info(self) -> Dict[str, str]:
        return {
            "catalog": self.catalog,
            "schema": self.schema,
            "volume_path": self.volume_path,
            "table": self.table,
            "sep": self.sep,
        }


class DatabricksIO:
    """Lightweight wrapper around DatabricksAPI for table IO."""

    def __init__(self, fname_databricks_env: str):
        self._obj_db = DatabricksAPI(fname_databricks_env=fname_databricks_env)

    def read_table(self, table_name: str) -> pd.DataFrame:
        sql = f"select * from {table_name}"
        return self._obj_db.query_from_sql(sql=sql)

    def write_table(self, df: pd.DataFrame, table_config: DatabricksTableConfig) -> None:
        self._obj_db.write_db_obj(
            df=df,
            volume_path=table_config.volume_path,
            sep=table_config.sep,
            overwrite=table_config.overwrite,
            dict_database_table_info=table_config.as_table_info(),
        )
