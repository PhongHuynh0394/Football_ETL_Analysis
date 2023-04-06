from dagster import asset, Output, AssetIn, AssetOut
import pandas as pd

tables = [
    "leagues",
    "players",
    "games",
    "appearances",
    "shots",
    "teamstats",
    "teams"
]


def asset_factory (table: str):
    @asset(
        name=table,
        io_manager_key="minio_io_manager",
        required_resource_keys={"mysql_io_manager"},
        key_prefix=["bronze", "ecom"],
        compute_kind="MySQL",
        group_name="bronze_layer"
    )
    def _asset(context) -> Output[pd.DataFrame]:
        sql_stm = f"SELECT * FROM {table}"
        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
        context.log
        return Output(
            pd_data,
            metadata={
                "table": table,
                "records": len(pd_data)
            }
        )
    
    return _asset
