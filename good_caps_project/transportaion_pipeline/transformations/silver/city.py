from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2

@dp.materialized_view(
    name="transportation.silver.city",
    comment="City silver Data transformation",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_silver():
    df_bronze = spark.read.table("transportation.bronze.city")
    df_silver = df_bronze.withColumnRenamed("ingest_datetime", "bronze_ingest_datetime")
    df_silver = df_silver.withColumn("silver_processed_timestamp", current_timestamp())
    return df_silver