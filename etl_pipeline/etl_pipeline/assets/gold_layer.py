from dagster import asset, Output, AssetIn
import pandas as pd

@asset(
    io_manager_key="minio_io_manager",
    ins={
        "dim_products": AssetIn(
            key_prefix=["silver", "ecom"]
        ),
        "fact_sales": AssetIn(
            key_prefix=["silver", "ecom"]
        )
    },
    group_name="Gold_layer",
    key_prefix=["gold", "ecom"],
    compute_kind="Minio"
)
def gold_sales_values_by_category(dim_products: pd.DataFrame, fact_sales: pd.DataFrame) -> Output[pd.DataFrame]:
    #copy in order not to make change to data
    di = dim_products.copy()
    fa = fact_sales.copy()

    #Create daily_sales_products after transform
    '''
    WITH daily_sales_products AS (
        SELECT
            CAST(order_purchase_timestamp AS DATE) AS daily,
            product_id,
            ROUND(SUM(CAST(payment_value AS FLOAT)), 2) AS sales,
            COUNT(DISTINCT(order_id)) AS bills
        FROM fact_sales
        WHERE order_status = 'delivered'
        GROUP BY
        CAST(order_purchase_timestamp AS DATE)
        , product_id
        )
    '''
    #filter only with condition in fact_sales
    fa = fa[fa["order_status"] == "delivered"]

    #convert order_purchase_timestamp into DATE format and set as daily
    fa['order_purchase_timestamp'] = pd.to_datetime(fa['order_purchase_timestamp'])
    fa['daily'] = fa['order_purchase_timestamp'].dt.date

    #Group
    daily_sales_products= (
        fa.groupby(["daily", "product_id"])
        .agg(sales=('payment_value', 'sum'), bills=('order_id', 'nunique'))
        .reset_index()
    )
    dp = daily_sales_products.copy()

    #Create daily_sales_categories table 
    '''
    SELECT
        ts.daily, 
        DATE_FORMAT(ts.daily, 'y-MM') AS monthly, 
        p.product_category_name_english AS category, 
        ts.sales, 
        ts.bills, 
        (ts.sales / ts.bills) AS values_per_bills
    FROM daily_sales_products ts
    JOIN dim_products p
    ON ts.product_id = p.product_id
    '''

    #merge 2 df on product_id
    daily_sales_categories = dp.merge(di, on="product_id")

    # Create some special column
    daily_sales_categories['monthly'] = pd.to_datetime(dp['daily']).dt.strftime('%Y-%m')
    daily_sales_categories = daily_sales_categories.rename(columns={"product_category_name_english": "category"})
    daily_sales_categories['values_per_bills'] = dp['sales']/dp["bills"]
    daily_sales_categories = daily_sales_categories[['daily', 'monthly', 'category', 'sales', 'bills', 'values_per_bills']]

    #copy
    dc = daily_sales_categories.copy()

    #Final result
    '''
    SELECT
        monthly, 
        category, 
        SUM(sales) AS total_sales, 
        SUM(bills) AS total_bills, 
        (SUM(sales) * 1.0 / SUM(bills)) AS values_per_bills
    FROM daily_sales_categories
    GROUP BY
        monthly,
        category
    '''
    result = (
    dc.groupby(["monthly", "category"])
    .agg(total_sales=("sales", "sum"), total_bills=("bills","sum")).reset_index()
    )
    result['values_per_bill'] = result['total_sales'] * 1.0 / result['total_bills']
    result= result[["monthly", "category", "total_sales", "total_bills", "values_per_bill"]]
    return Output(
        result,
        metadata={
            'table': 'Sales_values_by_category',
            'records': len(result),
            "type cols": str(result[result.columns.tolist()].dtypes)
        }
    )
