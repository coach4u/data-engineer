dbutils.fs.mount(
    source="wasbs://bronze@storacct201.blob.core.windows.net/",
    mount_point="/mnt/sales_project",
    extra_configs={
        "fs.azure.sas.bronze.storacct201.blob.core.windows.net": "sas toekn"
    }
)

display(dbutils.fs.ls("/mnt/sales_project/products"));
display(dbutils.fs.ls("/mnt/sales_project/stores"))
display(dbutils.fs.ls("/mnt/sales_project/transactions"))

df_product=spark.read.parquet('/mnt/sales_project/products/dbo.inventory_products.parquet');
df_store=spark.read.parquet('/mnt/sales_project/stores/dbo.retail_stores.parquet');
df_transaction=spark.read.parquet('/mnt/sales_project/transactions/dbo.sales_transactions.parquet');

from pyspark.sql.functions import col

# Convert types and clean data
df_transactions = df_transaction.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_product.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_store.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)



df_silver = df_transactions \
    .join(df_products, "product_id") \
    .join(df_stores, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))



dbutils.fs.unmount('/mnt/silver101')
dbutils.fs.mount(
    source="wasbs://silver@storacct201.blob.core.windows.net/",
    mount_point="/mnt/silver101",
    extra_configs={
        "fs.azure.sas.silver.storacct201.blob.core.windows.net": "sas token"
    
    }
)


dbutils.fs.ls("/mnt/silver101")
#display(df_silver)
# Write cleaned dataframes into silver layer
df_products.write.mode("overwrite").format("delta").save("/mnt/silver101/products")
df_stores.write.mode("overwrite").format("delta").save("/mnt/silver101/stores")
df_transactions.write.mode("overwrite").format("delta").save("/mnt/silver101/transactions")


%sql
select * from silver_transactions;
select * from silver_products;
select * from silver_stores





display(spark.sql("SELECT * FROM silver_products"))
display(spark.sql("SELECT * FROM silver_stores"))
display(spark.sql("SELECT * FROM silver_transactions"))




from pyspark.sql.functions import col

df_silver = df_transactions \
    .join(df_products, "product_id", "left") \
    .join(df_stores, "store_id", "left") \
    .withColumn("total_amount", col("quantity") * col("price"))



display(df_silver)



silver_base_path = "/mnt/silver101"
df_silver.write.mode("overwrite").format("delta").save(silver_base_path + "/transactions_enriched")
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_transactions_enriched
USING DELTA
LOCATION '/mnt/silver101/transactions_enriched'
""")




%sql SELECT * FROM silver_transactions_enriched;



#using python same as above cmd
df = spark.sql("SELECT * FROM silver_transactions_enriched")
display(df)
