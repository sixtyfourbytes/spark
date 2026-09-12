from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)
from pyspark.sql import functions as funcs

spark = (
    SparkSession.builder.appName("Customer Expendature")
    .master("local[*]")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("cust_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("price", FloatType(), True),
    ]
)

transactionDF = spark.read.schema(schema).csv("./DataFiles/customer-orders.csv")

spentByCustDf = (
    transactionDF.groupBy("cust_id")
    .agg(funcs.round(funcs.sum("price"), 2).alias("total_spent"))
    .sort("total_spent")
)

spentByCustDf.show()
