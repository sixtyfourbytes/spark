from pyspark.sql import SparkSession
import pyspark.pandas as ps
import pandas as pd

spark = (
    SparkSession.builder.appName("Pandas on Spark")
    .config("spark.sql.ansi.enable", "false")
    .getOrCreate()
)

# pandas
pd_df = pd.DataFrame(
    {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alic", "John", "Bob", "Charls", "David"],
        "age": [23, 28, 22, 34, 45],
    }
)

print(pd_df)

spark_df = spark.createDataFrame(pd_df)
spark_df.printSchema()
spark_df.show()
spark_df.filter(spark_df.age > 23).show()

spark_df.toPandas()

# pandas on spark for scalable pandas operation
ps_df = ps.DataFrame(pd_df)

print("using pandas on spark")
ps_df["age"] = ps_df["age"] + 2
print(ps_df)


def formatStr(x) -> str:
    return f"{x['name']}, {x['age']}"


# diff lenght from input
ps_df["name with age"] = ps_df.apply(formatStr, axis=1)
# same lenght from input
ps_df["age 10 years from now"] = ps_df.transform(lambda age: age + 10)

ps_df.to_spark().show()

spark.stop()
