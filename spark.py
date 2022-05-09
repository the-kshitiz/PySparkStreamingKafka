from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	

orders_schema = StructType([
                    StructField("invoice_no", StringType()),
                    StructField("country", StringType()),
                    StructField("timestamp", StringType()),
                    StructField("type", StringType()),
                    StructField("items", ArrayType(
                        StructType([
                            StructField("SKU", StringType()),
                            StructField("title", StringType()),
                            StructField("unit_price", StringType()),
                            StructField("quantity", StringType())
                        ])
                    ))
])

df = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","18.211.252.152:9092")  \
	.option("subscribe","real-time-project") \
    .option("startingOffsets","latest")  \
	.load() \
    .select(from_json(col("value").cast("string"), orders_schema).alias("orders"))

df1 = df.select("orders.*",explode("orders.items"))

df1 = df1.select("invoice_no", "country", "timestamp", "type", "col.*")


query = df \
    .select("invoice no", "country", "total_items", "is_order", "is_return") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime = "1 minute") \
    .start()

query.awaitTermination()
#Calculate time based KPI's

aggStreamByTimeNCountry = expandedOrderStream \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "1 minute")) \
    .agg(sum("total_cost"),
        avg("total_cost"),
        count("invoice_no").alias("OPM"),
        avg("is_return"))\
    .select("window",
            "OPM",
            format_number("sum(total_cost)", 2).alias("total_sale_volme"),
            format_number("avg(total_cost)", 2).alias("average_transaction_size"),
            format_number("avg(is_return)", 2).alias("rate_of_return"))

#Calculate time and country based KPI's
aggStreamByTimeNCountry = expandedOrderStream \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
    .agg(sum("total_cost"),
        count("invoice_no").alias("OPM"),
        avg("is_return"))\
    .select("window",
            "country",
            "OPM",
            format_number("sum(total_cost)", 2).alias("total_sale_volume"),
            format_number("avg(is_return)", 2).alias("rate_of_return"))



# Write time based KPI values
queryByTime = aggStreamByTime.writeStream \ 
        .format("json") \
        .outputMode("append") \
        .option("truncate", "false")
        .option("path", "time-wise-kpi1") \
        .option("checkpointLocation", "time-cp1") \
        .trigger(processingTime = "1 minute")
        .start()

# Write time based KPI values
queryByTimeNCountry = aggStreamByTimeNCountry.writeStream \ 
        .format("json") \
        .outputMode("append") \
        .option("truncate", "false")
        .option("path", "time-country-wise-kpi1") \
        .option("checkpointLocation", "time-country-cp1") \
        .trigger(processingTime = "1 minute")
        .start()

queryByTimeNCountry.awaitTermination()