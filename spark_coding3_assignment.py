from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("spark_coding3_assignment")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Read employee.json
    df = spark.read.json("hdfs:///user/data/employee.json")

    # 1. Remove duplicate employees
    df1 = df.dropDuplicates(["employee_name", "department", "salary"])

    # Write ORC file partitioned by department
    df1.write.mode("overwrite") \
        .partitionBy("department") \
        .orc("hdfs:///user/data/orc/employees_orc")

    # 2. Departments in descending order with mean salary
    df2 = df1.groupBy("department") \
        .agg(avg("salary").alias("mean_salary")) \
        .orderBy(col("mean_salary").desc())

    df2.show()

    spark.stop()
