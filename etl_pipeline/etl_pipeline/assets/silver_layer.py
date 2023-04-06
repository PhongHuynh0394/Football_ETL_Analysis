from dagster import asset, Output, AssetIn
import pandas as pd

@asset(
    io_manager_key="minio_io_manager",
    #required_resource_keys={"minio_io_manager"},
    ins={
        "bronze_olist_products_dataset": AssetIn(
            key_prefix=["bronze", "ecom"],
            input_manager_key="minio_io_manager"
        ),
        "bronze_product_category_name_translation": AssetIn(
            key_prefix=["bronze", "ecom"],
            input_manager_key="minio_io_manager"
        )
    },
    key_prefix=["silver","ecom"],
    compute_kind="Minio",
    group_name="Silver_layer"
)
def dim_products(bronze_olist_products_dataset: pd.DataFrame, bronze_product_category_name_translation: pd.DataFrame) -> Output[pd.DataFrame]:
    rp = bronze_olist_products_dataset.copy()
    pcnt = bronze_product_category_name_translation.copy()
    
    #merge data
    result = rp.merge(pcnt, on="product_category_name",how="inner")[["product_id", "product_category_name_english"]]
    return Output(
        result,
        metadata={
            'table': "dim_products",
            "records": len(result) 
        }
    )

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    ins={
        "bronze_olist_order_items_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        ),
        "bronze_olist_order_payments_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        ),
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    key_prefix=["silver", "ecom"],
    group_name="Silver_layer",
    compute_kind="Minio"
)
def fact_sales(bronze_olist_order_items_dataset: pd.DataFrame, bronze_olist_order_payments_dataset: pd.DataFrame, bronze_olist_orders_dataset: pd.DataFrame) -> Output[pd.DataFrame]:
    #set name for easier to understand
    ro = bronze_olist_orders_dataset.copy()
    roi = bronze_olist_order_items_dataset.copy()
    rop = bronze_olist_order_payments_dataset.copy()

    #merge those dataframe
    result = pd.merge(ro, roi, on="order_id", how="inner").merge(rop,on="order_id",how="inner")
    result = result[["order_id", "customer_id", "order_purchase_timestamp", "product_id", "payment_value", "order_status"]]
    return Output(
        result,
        metadata={
            "table": "fact_sales",
            "records": len(result)
        }
    )

