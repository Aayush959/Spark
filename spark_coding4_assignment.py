from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, count

if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("Q4_Hive_Partition")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Step 1: Read JSON files from HDFS
    employee = spark.read.json("hdfs:///data/employee.json")
    department = spark.read.json("hdfs:///data/department.json")

    # Step 2: Inner join employee & department
    join_df = employee.join(
        department,
        employee.emp_dept_id == department.dept_id,
        "inner"
    )

    # Step 3: Aggregate (max salary & employee count)
    aggregate_df = join_df.groupBy("dept_name").agg(
        max("salary").alias("maxSalary"),
        count("emp_id").alias("employeesCount")
    )

    aggregate_df.show()

    # Step 4: Write to Hive internal table (parquet + partition)
    aggregate_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("dept_name") \
        .saveAsTable("part_department")

    # Step 5: Verify table from Spark
    spark.sql("SHOW TABLES IN office_db").show()
