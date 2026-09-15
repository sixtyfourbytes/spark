from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf, udf
from pyspark.sql.types import IntegerType
import re


@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, text: str):
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield (hashtag,)


@udf(returnType=IntegerType())
def countHashtags(text: str):
    if text:
        return len(re.findall(r"#\w+", text))
    return 0


spark = (
    SparkSession.builder.appName("python udtf & udf example")
    .config("spark.sql.execution.pythonUDTF.enabled", "true")
    .getOrCreate()
)

spark.udtf.register("extractHashtag", HashtagExtractor)
spark.udf.register("countHashtags", countHashtags)

spark.sql("select * from extractHashtag('Welcome to #ApacheSpark and #BigData')").show()

spark.sql(
    "select countHashtags('Welcome to #ApacheSpark and #BigData') AS hashtagCount"
).show()

data = [
    ("Learning #DataEngineer",),
    ("Learning #ApacheSpark, #SQL, #Databricks",),
    ("No hashtags here",),
]
df = spark.createDataFrame(data, ["text"])

df.selectExpr("text", "countHashtags(text) as numHashtags").show()

df.createOrReplaceTempView('comments')
spark.sql('select text, hashtag from comments, LATERAL extractHashtag(text)').show()
