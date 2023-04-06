from dagster import asset, Output, AssetIn, AssetOut
import pandas as pd

ls_tables = [
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "product_category_name_translation"
]

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_order_items_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = f" SELECT * FROM {ls_tables[0]}"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

    return Output(
        pd_data,
        metadata={
            "table": "olist_order_items_dataset",
            "records": len(pd_data)
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_order_payments_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = f" SELECT * FROM {ls_tables[1]}"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

    return Output(
        pd_data,
        metadata={
            "table": "olist_order_payments_dataset",
            "records": len(pd_data)
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = f" SELECT * FROM {ls_tables[2]}"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

    return Output(
        pd_data,
        metadata={
            "table": "olist_orders_dataset",
            "records": len(pd_data)
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_products_dataset(context) -> Output[pd.DataFrame]:
    sql_stm = f" SELECT * FROM {ls_tables[3]}"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

    return Output(
        pd_data,
        metadata={
            "table": "olist_products_dataset",
            "records": len(pd_data)
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_product_category_name_translation(context) -> Output[pd.DataFrame]:
    sql_stm = f" SELECT * FROM {ls_tables[4]}"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

    return Output(
        pd_data,
        metadata={
            "table": "product_category_name_translation",
            "records": len(pd_data)
        }
    )