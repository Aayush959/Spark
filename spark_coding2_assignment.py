from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("spark_coding2_assignment") \
        .master("local[*]") \
        .getOrCreate()

    # Read student JSON file from HDFS (or local path)
    df = spark.read.json("hdfs:///user/data/student.json")

    # Filter students learning Java and not belonging to state OH
    df1 = df.filter(
        array_contains(df.languages, "Java") &
        (df.state != "OH")
    )

    # Select firstname and gender
    df1.select("name.firstname", "gender").show(truncate=False)

    spark.stop()
