from dagster import multi_asset, Output, AssetIn, AssetOut, asset
import pandas as pd

@multi_asset(
    ins={
        "gold_sales_values_by_category": AssetIn(
            key_prefix=["gold", "ecom"]
            #input_manager_key="minio_io_manger"
        )
    },
    outs={
        "sales_values_by_category": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["sales_values_by_category", 'gold'],
            metadata={
                "columns": [
                    "monthly",
                    "category",
                    "total_sales",
                    "total_bills",
                    "values_per_bill"
                ]
            }
        )
    },
    compute_kind="PostgreSQL",
    group_name="Warehouse_layer"
)
def sales_values_by_category(gold_sales_values_by_category: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
        gold_sales_values_by_category,
        metadata={
            "schema": "gold",
            "table": "sales_values_by_category",
            "records": len(gold_sales_values_by_category)
        }
    )