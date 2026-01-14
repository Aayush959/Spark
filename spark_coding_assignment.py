from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, LongType, DoubleType

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName('spark_coding_assignment')
        .master('local[*]')
        .enableHiveSupport()
        .getOrCreate()
    )
    schema = StructType([ \
        StructField("dept_name", StringType(), True), \
        StructField("dept_id", IntegerType(), True), \
        StructField("salary", LongType(), True), \
        ])
    df3 = spark.read.options(header='false', inferSchema='True', delimiter=',') \
    .schema(schema) \
        .csv("hdfs:///data/Department.txt")
    df3 =df3.withColumn("doubleSalary", col("salary")*2)
    df3.show()
    df3.printSchema()
    df3.write.mode("overwrite").parquet("hdfs:///data/department.parquet")

