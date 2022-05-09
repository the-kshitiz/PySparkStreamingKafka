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

df2 = df1 \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime = "1 minute") \
    .start()

df2.awaitTermination()